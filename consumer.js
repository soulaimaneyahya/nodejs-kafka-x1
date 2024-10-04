const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "nodejs-consumer",
  brokers: ["soulaimane:9092"],
});

const consumer = kafka.consumer({ groupId: "users-group" });

const run = async () => {
  await consumer.connect();
  console.log("Consumer connected");

  await consumer.subscribe({ topic: "users-topic", fromBeginning: true });
  console.log("Subscribed to users-topic");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received message: ${message.value.toString()}`);
    },
  });
};

run().catch(console.error);
