const { Kafka } = require('kafkajs');
const { SchemaRegistry, SchemaType } = require('@kafkajs/confluent-schema-registry');
const { Pool } = require('pg');

// Configuration from environment variables
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const SCHEMA_REGISTRY_URL = process.env.SCHEMA_REGISTRY_URL || 'http://schema-registry:8081';
const ACTUAL_TOPIC = process.env.ACTUAL_TOPIC || 'power-consumption-actual';
const PREDICTED_TOPIC = process.env.PREDICTED_TOPIC || 'power-consumption-predicted';
const WEATHER_TOPIC = process.env.WEATHER_TOPIC || 'weather-data';
const CONSUMER_GROUP = process.env.CONSUMER_GROUP || 'database-writer';
const DATABASE_URL = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/power_grid';
const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || '1000');
const FLUSH_INTERVAL = parseInt(process.env.FLUSH_INTERVAL || '10000'); // 10 seconds

console.log('Starting Database Writer Service');
console.log('Configuration:');
console.log('- Kafka Brokers:', KAFKA_BROKERS);
console.log('- Schema Registry:', SCHEMA_REGISTRY_URL);
console.log('- Topics:', [ACTUAL_TOPIC, PREDICTED_TOPIC, WEATHER_TOPIC]);
console.log('- Consumer Group:', CONSUMER_GROUP);
console.log('- Batch Size:', BATCH_SIZE);
console.log('- Flush Interval:', FLUSH_INTERVAL, 'ms');

// Initialize PostgreSQL connection pool
const pool = new Pool({
  connectionString: DATABASE_URL,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 10000,
});

// Test database connection
pool.on('connect', () => {
  console.log('Database pool connected');
});

pool.on('error', (err) => {
  console.error('Unexpected error on idle database client', err);
  process.exit(-1);
});

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'database-writer',
  brokers: KAFKA_BROKERS,
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

// Initialize Schema Registry
const registry = new SchemaRegistry({ host: SCHEMA_REGISTRY_URL });

const consumer = kafka.consumer({ 
  groupId: CONSUMER_GROUP,
  sessionTimeout: 30000,
  heartbeatInterval: 3000
});

// In-memory batch storage
let batch = [];
let lastFlushTime = Date.now();
let totalWritten = 0;
let totalErrors = 0;

// Statistics
let stats = {
  messagesReceived: 0,
  messagesWritten: 0,
  batchesWritten: 0,
  errors: 0,
  lastFlush: new Date().toISOString()
};

// Flush batch to database
async function flushBatch() {
  if (batch.length === 0) return;
  
  // Deduplicate batch: keep only the last entry for each (time, type) pair
  const uniqueEntries = new Map();
  batch.forEach(item => {
    const key = `${item.time}_${item.type}`;
    uniqueEntries.set(key, item);
  });

  const batchToFlush = Array.from(uniqueEntries.values());
  batch = []; // Clear the original batch
  
  const startTime = Date.now();
  
  try {
    // Build parameterized INSERT query
    const values = batchToFlush.map((_, i) => 
      `($${i*3+1}, $${i*3+2}, $${i*3+3})`
    ).join(',');
    
    const params = batchToFlush.flatMap(item => [
      item.time,
      item.value,
      item.type
    ]);
    
    const query = `
      INSERT INTO measurements (time, value, type) 
      VALUES ${values}
      ON CONFLICT (time, type) DO UPDATE 
      SET value = EXCLUDED.value
    `;
    
    await pool.query(query, params);
    
    const duration = Date.now() - startTime;
    totalWritten += batchToFlush.length;
    stats.messagesWritten += batchToFlush.length;
    stats.batchesWritten += 1;
    stats.lastFlush = new Date().toISOString();
    lastFlushTime = Date.now();
    
    console.log(`✓ Flushed ${batchToFlush.length} records in ${duration}ms (total: ${totalWritten})`);
  } catch (error) {
    console.error('✗ Database write error:', error.message);
    totalErrors++;
    stats.errors += 1;
    
    // Re-add to batch for retry (optional - implement dead letter queue for production)
    // Note: We re-add the deduplicated batch to avoid infinite loops with duplicates
    batch = [...batchToFlush, ...batch];
    
    // If batch is too large after error, drop oldest records
    if (batch.length > BATCH_SIZE * 3) {
      const dropped = batch.length - BATCH_SIZE;
      batch = batch.slice(-BATCH_SIZE);
      console.warn(`⚠ Dropped ${dropped} old records due to repeated errors`);
    }
  }
}

// Periodic flush
const flushTimer = setInterval(async () => {
  const timeSinceLastFlush = Date.now() - lastFlushTime;
  
  if (batch.length > 0 && timeSinceLastFlush >= FLUSH_INTERVAL) {
    await flushBatch();
  }
}, 1000); // Check every second

// Kafka consumer setup
async function setupKafka() {
  try {
    await consumer.connect();
    console.log('✓ Connected to Kafka');

    await consumer.subscribe({ 
      topics: [ACTUAL_TOPIC, PREDICTED_TOPIC, WEATHER_TOPIC],
      fromBeginning: false 
    });
    console.log(`✓ Subscribed to topics: ${ACTUAL_TOPIC}, ${PREDICTED_TOPIC}, ${WEATHER_TOPIC}`);

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        try {
          let value;

          // Try to decode with Schema Registry first
          try {
            value = await registry.decode(message.value);
          } catch (e) {
            // Fallback to JSON if not Avro (e.g. legacy messages or replay script)
            value = JSON.parse(message.value.toString());
          }

          let type = '';
          if (topic === ACTUAL_TOPIC) {
            type = 'actual';
          } else if (topic === PREDICTED_TOPIC) {
            type = 'predicted';
          } else if (topic === WEATHER_TOPIC) {
            type = 'weather';
          }

          // Extract value based on message structure
          let numericValue = 0;
          if (value.value !== undefined) {
            numericValue = parseFloat(value.value);
          } else if (value.total_consumption_mwh !== undefined) {
            numericValue = parseFloat(value.total_consumption_mwh);
          } else if (value.total_production_mwh !== undefined) {
            numericValue = parseFloat(value.total_production_mwh);
          } else if (value.predictions && value.predictions.consumption_mwh !== undefined) {
             numericValue = parseFloat(value.predictions.consumption_mwh);
          }

          // Add to batch
          batch.push({
            time: value.timestamp || new Date().toISOString(),
            value: numericValue,
            type: type
          });
          
          stats.messagesReceived += 1;
          
          // Flush if batch size reached
          if (batch.length >= BATCH_SIZE) {
            await flushBatch();
          }
        } catch (error) {
          console.error('✗ Error processing message:', error.message);
          stats.errors += 1;
        }
      },
    });
    
    console.log('✓ Database writer service running');
  } catch (error) {
    console.error('✗ Error setting up Kafka:', error);
    console.log('Retrying in 5 seconds...');
    setTimeout(setupKafka, 5000);
  }
}

// Health check and stats endpoint (simple HTTP server)
const http = require('http');
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'healthy',
      batch: {
        current: batch.length,
        maxSize: BATCH_SIZE
      },
      stats: stats,
      uptime: process.uptime()
    }));
  } else if (req.url === '/metrics') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end(`
# Database Writer Metrics
messages_received_total ${stats.messagesReceived}
messages_written_total ${stats.messagesWritten}
batches_written_total ${stats.batchesWritten}
errors_total ${stats.errors}
batch_current_size ${batch.length}
    `.trim());
  } else {
    res.writeHead(404);
    res.end('Not Found');
  }
});

const HEALTH_PORT = process.env.HEALTH_PORT || 8081;
healthServer.listen(HEALTH_PORT, () => {
  console.log(`Health check server running on port ${HEALTH_PORT}`);
  console.log(`- http://localhost:${HEALTH_PORT}/health`);
  console.log(`- http://localhost:${HEALTH_PORT}/metrics`);
});

// Graceful shutdown
async function gracefulShutdown() {
  console.log('Shutting down gracefully...');
  
  clearInterval(flushTimer);
  
  // Flush remaining batch
  if (batch.length > 0) {
    console.log(`Flushing ${batch.length} remaining records...`);
    await flushBatch();
  }
  
  await consumer.disconnect();
  await pool.end();
  
  console.log('✓ Shutdown complete');
  process.exit(0);
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Start the service
setupKafka();
