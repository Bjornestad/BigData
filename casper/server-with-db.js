const { Kafka } = require('kafkajs');
const WebSocket = require('ws');
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Pool } = require('pg');

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

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'power-grid-backend',
  brokers: KAFKA_BROKERS,
});

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

// Initialize PostgreSQL pool (if DATABASE_URL is provided)
let pool = null;
if (DATABASE_URL) {
  pool = new Pool({
    connectionString: DATABASE_URL,
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 10000,
  });
  
  pool.on('error', (err) => {
    console.error('Unexpected error on idle database client', err);
  });
  
  console.log('Database connection pool initialized');
} else {
  console.warn('DATABASE_URL not set - historical queries disabled');
}

// Express app for health checks and API
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy',
    actualDataPoints: actualData.length,
    predictedDataPoints: predictedData.length,
    databaseEnabled: !!pool
  });
});

// Current data endpoint (in-memory buffer)
app.get('/data', (req, res) => {
  res.json({
    actual: actualData,
    predicted: predictedData
  });
});

// Historical data endpoint (from database)
app.post('/api/historical', async (req, res) => {
  if (!pool) {
    return res.status(503).json({ 
      error: 'Database not configured',
      message: 'Historical queries require DATABASE_URL to be set'
    });
  }
  
  try {
    const { start, end, type, interval } = req.body;
    
    if (!start || !end) {
      return res.status(400).json({ 
        error: 'Missing required parameters',
        message: 'start and end timestamps are required'
      });
    }
    
    const startDate = new Date(start);
    const endDate = new Date(end);
    
    // Validate dates
    if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) {
      return res.status(400).json({ 
        error: 'Invalid date format',
        message: 'start and end must be valid ISO 8601 timestamps'
      });
    }
    
    // Auto-select interval based on time range if not provided
    let selectedInterval = interval;
    if (!selectedInterval) {
      const duration = endDate - startDate;
      const hours = duration / (1000 * 60 * 60);
      
      if (hours <= 1) {
        selectedInterval = '1 minute';
      } else if (hours <= 24) {
        selectedInterval = '5 minutes';
      } else if (hours <= 168) { // 1 week
        selectedInterval = '1 hour';
      } else {
        selectedInterval = '1 day';
      }
    }
    
    // Decide which table/view to use based on interval
    let tableName;
    if (selectedInterval.includes('minute')) {
      tableName = 'measurements'; // Raw data
    } else if (selectedInterval.includes('hour') || selectedInterval.includes('day')) {
      // Check if we can use pre-aggregated views
      if (selectedInterval === '5 minutes') {
        tableName = 'measurements_5min';
      } else if (selectedInterval === '1 hour') {
        tableName = 'measurements_1hour';
      } else if (selectedInterval === '1 day') {
        tableName = 'measurements_1day';
      } else {
        tableName = 'measurements'; // Fallback to raw
      }
    } else {
      tableName = 'measurements';
    }
    
    // Build query
    let query, params;
    
    if (tableName.includes('measurements_')) {
      // Query pre-aggregated view
      query = `
        SELECT 
          bucket as timestamp,
          type,
          avg_value as value,
          min_value as min,
          max_value as max,
          count
        FROM ${tableName}
        WHERE bucket >= $1 AND bucket < $2
          AND ($3::varchar IS NULL OR type = $3)
        ORDER BY bucket ASC
      `;
      params = [startDate, endDate, type || null];
    } else {
      // Query raw data with aggregation
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
      params = [selectedInterval, startDate, endDate, type || null];
    }
    
    const result = await pool.query(query, params);
    
    res.json({
      data: result.rows,
      metadata: {
        interval: selectedInterval,
        source: tableName,
        pointsReturned: result.rows.length,
        timeRange: {
          start: startDate.toISOString(),
          end: endDate.toISOString()
        }
      }
    });
  } catch (error) {
    console.error('Error querying historical data:', error);
    res.status(500).json({ 
      error: 'Database query failed',
      message: error.message
    });
  }
});

// Stats endpoint (from database)
app.post('/api/stats', async (req, res) => {
  if (!pool) {
    return res.status(503).json({ 
      error: 'Database not configured'
    });
  }
  
  try {
    const { start, end, type } = req.body;
    
    const startDate = new Date(start || Date.now() - 24*60*60*1000);
    const endDate = new Date(end || Date.now());
    
    const result = await pool.query(`
      SELECT 
        type,
        AVG(value) as avg,
        MIN(value) as min,
        MAX(value) as max,
        STDDEV(value) as stddev,
        COUNT(*) as count
      FROM measurements
      WHERE time >= $1 AND time < $2
        AND ($3::varchar IS NULL OR type = $3)
      GROUP BY type
    `, [startDate, endDate, type || null]);
    
    res.json(result.rows);
  } catch (error) {
    console.error('Error querying stats:', error);
    res.status(500).json({ error: 'Database query failed' });
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
  console.log('Client connected (total:', wss.clients.size, ')');
  
  // Send existing data to new client
  ws.send(JSON.stringify({
    type: 'initial',
    actual: actualData,
    predicted: predictedData
  }));

  ws.on('close', () => {
    console.log('Client disconnected (remaining:', wss.clients.size - 1, ')');
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
            
            console.log('Actual:', dataPoint.value.toFixed(2), 'MW');
          } else if (topic === PREDICTED_TOPIC) {
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
            
            console.log('Predicted:', dataPoint.value.toFixed(2), 'MW');
          }
        } catch (error) {
          console.error('Error processing message:', error);
        }
      },
    });
  } catch (error) {
    console.error('Error setting up Kafka:', error);
    setTimeout(setupKafka, 5000);
  }
}

// Start server
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}`);
  console.log(`HTTP API: http://localhost:${PORT}`);
  console.log('');
  setupKafka();
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server and Kafka consumer');
  await consumer.disconnect();
  if (pool) await pool.end();
  server.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
});
