const { Kafka } = require('kafkajs');

// Configuration
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const ACTUAL_TOPIC = process.env.ACTUAL_TOPIC || 'energy_actual';
const PREDICTED_TOPIC = process.env.PREDICTED_TOPIC || 'energy_predictions';
const INTERVAL_MS = parseInt(process.env.INTERVAL_MS || '2000');

// Initialize Kafka
const kafka = new Kafka({
  clientId: 'test-producer',
  brokers: KAFKA_BROKERS,
});

const producer = kafka.producer();

// Simulate realistic power consumption patterns
class PowerConsumptionSimulator {
  constructor() {
    this.baseLoad = 1000; // MW
    this.time = 0;
    this.noise = 0;
  }

  // Generate actual consumption with realistic patterns
  getActualConsumption() {
    // Time-of-day pattern (higher during day, lower at night)
    const hour = (new Date().getHours() + this.time / 60) % 24;
    const timeOfDayFactor = 0.3 + 0.7 * Math.sin((hour - 6) * Math.PI / 12);
    
    // Weekly pattern (lower on weekends)
    const dayOfWeek = new Date().getDay();
    const weekdayFactor = (dayOfWeek >= 1 && dayOfWeek <= 5) ? 1.0 : 0.85;
    
    // Random noise and variations
    this.noise = this.noise * 0.9 + (Math.random() - 0.5) * 50;
    const randomVariation = Math.sin(this.time / 10) * 100 + this.noise;
    
    // Occasional spikes (industrial loads)
    const spike = (Math.random() > 0.95) ? Math.random() * 200 : 0;
    
    const consumption = this.baseLoad * timeOfDayFactor * weekdayFactor + randomVariation + spike;
    
    this.time += 1;
    return Math.max(200, consumption); // Minimum 200 MW
  }

  // Generate predicted consumption with some variance
  getPredictedConsumption(actualValue) {
    // Prediction is close to actual but with some error
    const predictionError = (Math.random() - 0.5) * 100;
    const trendError = Math.sin(this.time / 20) * 50;
    
    return actualValue + predictionError + trendError;
  }
}

async function run() {
  await producer.connect();
  console.log('Connected to Kafka');
  console.log(`Publishing to topics: ${ACTUAL_TOPIC}, ${PREDICTED_TOPIC}`);
  console.log(`Interval: ${INTERVAL_MS}ms`);

  const simulator = new PowerConsumptionSimulator();

  // Generate current and future predictions
  setInterval(async () => {
    const now = new Date();
    
    // Generate actual consumption for now
    const actualValue = simulator.getActualConsumption();
    const actualMessage = {
      timestamp: now.toISOString(),
      value: parseFloat(actualValue.toFixed(2))
    };

    // Send actual consumption
    await producer.send({
      topic: ACTUAL_TOPIC,
      messages: [
        { value: JSON.stringify(actualMessage) }
      ],
    });

    console.log(`Actual: ${actualValue.toFixed(2)} MW at ${now.toISOString()}`);

    // Generate predictions for the next 30 minutes (every 2 minutes)
    for (let i = 1; i <= 15; i++) {
      const futureTime = new Date(now.getTime() + i * 2 * 60 * 1000);
      const futureActual = simulator.getActualConsumption();
      const predictedValue = simulator.getPredictedConsumption(futureActual);
      
      const predictedMessage = {
        timestamp: futureTime.toISOString(),
        value: parseFloat(predictedValue.toFixed(2))
      };

      // Send predicted consumption
      await producer.send({
        topic: PREDICTED_TOPIC,
        messages: [
          { value: JSON.stringify(predictedMessage) }
        ],
      });
    }

    console.log(`Generated predictions for next 30 minutes`);
    console.log('---');
  }, INTERVAL_MS);
}

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM signal received: closing Kafka producer');
  await producer.disconnect();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('SIGINT signal received: closing Kafka producer');
  await producer.disconnect();
  process.exit(0);
});

// Start producing
run().catch(async (error) => {
  console.error('Error:', error);
  try {
    await producer.disconnect();
  } catch (e) {
    console.error('Error disconnecting producer:', e);
  }
  process.exit(1);
});
