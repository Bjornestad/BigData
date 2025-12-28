const { Kafka } = require('kafkajs');
const WebSocket = require('ws');
const express = require('express');
const http = require('http');

// Configuration from environment variables
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const ACTUAL_TOPIC = process.env.ACTUAL_TOPIC || 'power-consumption-actual';
const PREDICTED_TOPIC = process.env.PREDICTED_TOPIC || 'power-consumption-predicted';
const PORT = process.env.PORT || 8080;
const CONSUMER_GROUP = process.env.CONSUMER_GROUP || 'power-grid-monitor';

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

// Express app for health checks
const app = express();
const server = http.createServer(app);

app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy',
    actualDataPoints: actualData.length,
    predictedDataPoints: predictedData.length
  });
});

app.get('/data', (req, res) => {
  res.json({
    actual: actualData,
    predicted: predictedData
  });
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
  setupKafka();
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing HTTP server and Kafka consumer');
  await consumer.disconnect();
  server.close(() => {
    console.log('HTTP server closed');
    process.exit(0);
  });
});
