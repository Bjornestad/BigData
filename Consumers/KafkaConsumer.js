const { Kafka } = require('kafkajs')

const kafka = new Kafka({
    clientId: 'frontend-api',
    brokers: ['localhost:9092']
})

const consumer = kafka.consumer({ groupId: 'frontend-live-dashboard' })

const run = async () => {
    // 1. Connect
    await consumer.connect()

    // 2. Subscribe
    await consumer.subscribe({ topic: 'energy-predictions-output', fromBeginning: false })

    // 3. Loop
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                value: message.value.toString(),
            })
            //push data to the frontend
        },
    })
}

run().catch(console.error)