const { Kafka } = require('kafkajs');
const WebSocket = require('ws');
const express = require('express');
const http = require('http');
const { Pool } = require('pg');
const cors = require('cors');

// Configuration from environment variables
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const ACTUAL_TOPIC = process.env.ACTUAL_TOPIC || 'energy_actual';
const PREDICTED_TOPIC = process.env.PREDICTED_TOPIC || 'energy_predictions';
const PORT = process.env.PORT || 8080;
const CONSUMER_GROUP = process.env.CONSUMER_GROUP || 'power-grid-monitor';
const DATABASE_URL = process.env.DATABASE_URL;

// In-memory data store (last 100 data points)
const MAX_DATA_POINTS = 100;
let actualData = [];
let predictedData = [];

// Initialize PostgreSQL connection pool (optional - only if DATABASE_URL is provided)
let pool = null;
if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  });

  pool.query('SELECT NOW()', (err, res) => {
    if (err) {
      console.error('Database connection error:', err);
    } else {
      console.log('Database connected successfully');
    }
  });
}

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'power-grid-backend',
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

// Express app setup
const app = express();
const server = http.createServer(app);

// Middleware
app.use(cors());
app.use(express.json());

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy',
    actualDataPoints: actualData.length,
    predictedDataPoints: predictedData.length,
    databaseConnected: pool !== null
  });
});

// Current in-memory data endpoint
app.get('/data', (req, res) => {
  res.json({
    actual: actualData,
    predicted: predictedData
  });
});

// Historical data endpoint (requires database)
app.post('/api/historical', async (req, res) => {
  if (!pool) {
    return res.status(503).json({ 
      error: 'Database not configured',
      message: 'Historical data requires DATABASE_URL to be set'
    });
  }

  try {
    const { start, end, type, interval } = req.body;
    
    if (!start || !end) {
      return res.status(400).json({ error: 'start and end timestamps required' });
    }

    const startDate = new Date(start);
    const endDate = new Date(end);
    const duration = endDate - startDate;

    // Auto-select interval based on time range if not specified
    let queryInterval = interval;
    if (!queryInterval) {
      if (duration < 60 * 60 * 1000) {
        // < 1 hour: raw data
        queryInterval = 'raw';
      } else if (duration < 24 * 60 * 60 * 1000) {
        // < 24 hours: 1 minute
        queryInterval = '1 minute';
      } else if (duration < 7 * 24 * 60 * 60 * 1000) {
        // < 7 days: 5 minutes
        queryInterval = '5 minutes';
      } else if (duration < 30 * 24 * 60 * 60 * 1000) {
        // < 30 days: 1 hour
        queryInterval = '1 hour';
      } else {
        // > 30 days: 1 day
        queryInterval = '1 day';
      }
    }

    let query, params;

    if (queryInterval === 'raw') {
      // Query raw data
      query = `
        SELECT 
          time as timestamp,
          value,
          type
        FROM measurements
        WHERE time >= $1 AND time < $2
          AND ($3::varchar IS NULL OR type = $3)
        ORDER BY time ASC
        LIMIT 10000
      `;
      params = [startDate, endDate, type || null];
    } else {
      // Query aggregated data
      query = `
        SELECT 
          time_bucket($1, time) AS timestamp,
          type,
          AVG(value) as value,
          MIN(value) as min,
          MAX(value) as max,
          COUNT(*) as count
        FROM measurements
        WHERE time >= $2 AND time < $3
          AND ($4::varchar IS NULL OR type = $4)
        GROUP BY timestamp, type
        ORDER BY timestamp ASC
      `;
      params = [queryInterval, startDate, endDate, type || null];
    }

    const result = await pool.query(query, params);

    res.json({
      data: result.rows,
      interval: queryInterval,
      count: result.rows.length
    });

  } catch (error) {
    console.error('Historical query error:', error);
    res.status(500).json({ 
      error: 'Database query failed',
      message: error.message 
    });
  }
});

// Statistics endpoint (requires database)
app.post('/api/stats', async (req, res) => {
  if (!pool) {
    return res.status(503).json({ error: 'Database not configured' });
  }

  try {
    const { start, end, type } = req.body;
    
    if (!start || !end) {
      return res.status(400).json({ error: 'start and end timestamps required' });
    }

    const query = `
      SELECT 
        type,
        COUNT(*) as count,
        AVG(value) as avg,
        MIN(value) as min,
        MAX(value) as max,
        STDDEV(value) as stddev
      FROM measurements
      WHERE time >= $1 AND time < $2
        AND ($3::varchar IS NULL OR type = $3)
      GROUP BY type
    `;

    const result = await pool.query(query, [new Date(start), new Date(end), type || null]);

    res.json({
      stats: result.rows
    });

  } catch (error) {
    console.error('Stats query error:', error);
    res.status(500).json({ 
      error: 'Database query failed',
      message: error.message 
    });
  }
});

// WebSocket server
const wss = new WebSocket.Server({ server });

// Broadcast to all connected clients
function broadcast(data) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

wss.on('connection', (ws) => {
  console.log('Client connected');
  
  // Send existing data to new client
  ws.send(JSON.stringify({
    type: 'initial',
    actual: actualData,
    predicted: predictedData
  }));

  ws.on('close', () => {
    console.log('Client disconnected');
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
});

// Kafka consumer setup
async function setupKafka() {
  try {
    await consumer.connect();
    console.log('Connected to Kafka');

    await consumer.subscribe({ topics: [ACTUAL_TOPIC, PREDICTED_TOPIC], fromBeginning: false });
    console.log(`Subscribed to topics: ${ACTUAL_TOPIC}, ${PREDICTED_TOPIC}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          const value = JSON.parse(message.value.toString());
          
          if (topic === ACTUAL_TOPIC) {
            // Expected format: { timestamp: ISO8601, value: number }
            const dataPoint = {
              timestamp: value.timestamp || new Date().toISOString(),
              value: parseFloat(value.value),
              type: 'actual'
            };
            
            actualData.push(dataPoint);
            if (actualData.length > MAX_DATA_POINTS) {
              actualData.shift();
            }
            
            broadcast({
              type: 'actual',
              data: dataPoint
            });
            
            console.log('Actual:', dataPoint);
          } else if (topic === PREDICTED_TOPIC) {
            // Expected format: { timestamp: ISO8601, value: number }
            const dataPoint = {
              timestamp: value.timestamp || new Date().toISOString(),
              value: parseFloat(value.value),
              type: 'predicted'
            };
            
            predictedData.push(dataPoint);
            if (predictedData.length > MAX_DATA_POINTS) {
              predictedData.shift();
            }
            
            broadcast({
              type: 'predicted',
              data: dataPoint
            });
            
            console.log('Predicted:', dataPoint);
          }
        } catch (error) {
          console.error('Error processing message:', error);
        }
      },
    });
  } catch (error) {
    console.error('Error setting up Kafka:', error);
    // Retry connection after 5 seconds
    setTimeout(setupKafka, 5000);
  }
}

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}`);
  console.log(`HTTP API: http://localhost:${PORT}`);
  if (DATABASE_URL) {
    console.log('Historical data API enabled');
  } else {
    console.log('Historical data API disabled (no DATABASE_URL)');
  }
  setupKafka();
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server and Kafka consumer');
  await consumer.disconnect();
  if (pool) {
    await pool.end();
  }
  server.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
});
