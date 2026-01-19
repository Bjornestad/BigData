const { Kafka } = require('kafkajs');
const { SchemaRegistry } = require('@kafkajs/confluent-schema-registry');
const WebSocket = require('ws');
const express = require('express');
const http = require('http');
const cors = require('cors');
const { Pool } = require('pg');
const { URL } = require('url');

// Configuration from environment variables
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://schema-registry:8081';
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
  retry: {
    initialRetryTime: 300,
    retries: 15
  }
});

// Initialize Schema Registry
const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });

const consumer = kafka.consumer({ groupId: CONSUMER_GROUP });

// --- Database Connection with Retry Logic ---
let pool = null;

async function initializeDatabase() {
  if (!DATABASE_URL) {
    console.warn('DATABASE_URL not set - historical queries disabled');
    return;
  }

  const maxRetries = 30;
  const retryDelay = 5000; // 5 seconds

  for (let i = 0; i < maxRetries; i++) {
    try {
      console.log(`Attempting database connection (Attempt ${i + 1}/${maxRetries})...`);

      // Manually parse the URL
      const dbUrl = new URL(DATABASE_URL);
      const config = {
        user: dbUrl.username,
        password: decodeURIComponent(dbUrl.password),
        host: dbUrl.hostname,
        port: parseInt(dbUrl.port) || 5432,
        database: dbUrl.pathname.slice(1),
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000,
      };

      const tempPool = new Pool(config);

      // Test connection
      await tempPool.query('SELECT NOW()');

      console.log('Database connection established successfully.');
      pool = tempPool;

      pool.on('error', (err) => {
        console.error('Unexpected error on idle database client', err);
      });

      return; // Success
    } catch (error) {
      console.error(`Database connection failed: ${error.message}`);
      if (i < maxRetries - 1) {
        console.log(`Retrying in ${retryDelay/1000} seconds...`);
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }
  }

  console.error('Could not connect to database after multiple attempts. Historical data will be unavailable.');
}

// --- Load Initial Data from DB (Hydration) ---
async function loadInitialData() {
  if (!pool) return;

  try {
    console.log('Loading initial data from database...');
    // Look back 48 hours to ensure we catch data even if simulation is slightly ahead/behind
    const cutoffTime = Date.now() - (48 * 60 * 60 * 1000);

    // 1. Load Actual Data
    const actualRes = await pool.query(`
      SELECT time, value, type 
      FROM measurements 
      WHERE type = 'actual' 
      ORDER BY time DESC 
      LIMIT $1
    `, [MAX_DATA_POINTS]);

    if (actualRes.rows.length > 0) {
      actualData = actualRes.rows
          .filter(row => new Date(row.time).getTime() > cutoffTime)
          .map(row => ({
            timestamp: new Date(row.time).toISOString(),
            value: parseFloat(row.value),
            type: 'actual',
            production: 0,
            dk_area: 'DK1'
          }))
          .reverse();
    }

    // 2. Load Predicted Data
    const predRes = await pool.query(`
      SELECT time, value, type 
      FROM measurements 
      WHERE type = 'predicted' 
      ORDER BY time DESC 
      LIMIT $1
    `, [MAX_DATA_POINTS]);

    if (predRes.rows.length > 0) {
      predictedData = predRes.rows
          .filter(row => new Date(row.time).getTime() > cutoffTime)
          .map(row => ({
            timestamp: new Date(row.time).toISOString(),
            value: parseFloat(row.value),
            type: 'predicted',
            production: 0,
            net_balance: 0
          }))
          .reverse();
    }

    console.log(`Initialized ${actualData.length} actual and ${predictedData.length} predicted points from DB.`);
  } catch (error) {
    console.error('Error loading initial data:', error);
  }
}

// Express app
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);

// Health check
app.get('/health', (req, res) => {
  res.json({
    status: 'healthy',
    actualDataPoints: actualData.length,
    predictedDataPoints: predictedData.length,
    databaseEnabled: !!pool
  });
});

// Current data
app.get('/data', (req, res) => {
  res.json({
    actual: actualData,
    predicted: predictedData
  });
});

// Historical data endpoint (Fixed to always use raw table)
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

    // FIX: Always use the raw 'measurements' table.
    const tableName = 'measurements';

    const query = `
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
    const params = [selectedInterval, startDate, endDate, type || null];

    console.log(`[DEBUG] Querying historical data: ${selectedInterval}, Table: ${tableName}, Range: ${startDate.toISOString()} - ${endDate.toISOString()}`);

    const result = await pool.query(query, params);

    console.log(`[DEBUG] Query returned ${result.rows.length} rows.`);

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

// Stats endpoint
app.post('/api/stats', async (req, res) => {
  if (!pool) return res.status(503).json({ error: 'Database not configured' });

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

function broadcast(data) {
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
}

wss.on('connection', (ws) => {
  console.log('Client connected (total:', wss.clients.size, ')');
  ws.send(JSON.stringify({
    type: 'initial',
    actual: actualData,
    predicted: predictedData
  }));
});

// Kafka consumer setup
async function setupKafka() {
  try {
    await consumer.connect();
    console.log('Connected to Kafka');
    await consumer.subscribe({ topics: [ACTUAL_TOPIC, PREDICTED_TOPIC], fromBeginning: false });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          let value;
          try {
            value = await registry.decode(message.value);
          } catch (e) {
            value = JSON.parse(message.value.toString());
          }

          if (topic === ACTUAL_TOPIC) {
            const timestamp = value.timestamp || value.HourUTC || value.HourDK || new Date().toISOString();

            // Filter out old data (Replay script protection)
            const msgTime = new Date(timestamp).getTime();
            const cutoffTime = Date.now() - (24 * 60 * 60 * 1000);
            if (msgTime < cutoffTime) return;

            const incomingValue = parseFloat(value.total_consumption_mwh || value.value || value.ShareMWh || 0);
            const incomingProduction = parseFloat(value.total_production_mwh || 0);

            const existingPoint = actualData.find(d => d.timestamp === timestamp);
            let dataPoint;

            if (existingPoint) {
              existingPoint.value += incomingValue;
              existingPoint.production += incomingProduction;
              dataPoint = existingPoint;
            } else {
              dataPoint = {
                timestamp: timestamp,
                value: incomingValue,
                production: incomingProduction,
                dk_area: value.dk_area || value.PriceArea,
                type: 'actual'
              };
              actualData.push(dataPoint);
              if (actualData.length > MAX_DATA_POINTS) actualData.shift();
            }

            broadcast({ type: 'actual', data: dataPoint });
            console.log(`Actual [${timestamp}]: ${dataPoint.value.toFixed(2)} MW`);

          } else if (topic === PREDICTED_TOPIC) {
            const timestamp = value.timestamp || value.HourUTC || value.HourDK || new Date().toISOString();

            const msgTime = new Date(timestamp).getTime();
            const cutoffTime = Date.now() - (24 * 60 * 60 * 1000);
            if (msgTime < cutoffTime) return;

            const incomingValue = parseFloat(value.predictions?.consumption_mwh || value.value || 0);
            const incomingProduction = parseFloat(value.predictions?.production_mwh || 0);
            const incomingNet = parseFloat(value.predictions?.net_balance_mwh || 0);

            const existingPoint = predictedData.find(d => d.timestamp === timestamp);
            let dataPoint;

            if (existingPoint) {
              existingPoint.value += incomingValue;
              existingPoint.production += incomingProduction;
              existingPoint.net_balance += incomingNet;
              dataPoint = existingPoint;
            } else {
              dataPoint = {
                timestamp: timestamp,
                value: incomingValue,
                production: incomingProduction,
                net_balance: incomingNet,
                dk_area: value.dk_area,
                type: 'predicted'
              };
              predictedData.push(dataPoint);
              if (predictedData.length > MAX_DATA_POINTS) predictedData.shift();
            }

            broadcast({ type: 'predicted', data: dataPoint });
            console.log(`Predicted [${timestamp}]: ${dataPoint.value.toFixed(2)} MW`);
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
server.listen(PORT, async () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`WebSocket endpoint: ws://localhost:${PORT}`);
  console.log(`HTTP API: http://localhost:${PORT}`);
  console.log('');

  // 1. Initialize Database (with retry)
  await initializeDatabase();

  // 2. Load initial data from DB
  await loadInitialData();

  // 3. Start Kafka Consumer
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