const { Kafka, Partitioners } = require("kafkajs");

const x = process.argv[2]; // Message to send

async function run() {
  const kafka = new Kafka({
    clientId: "nodejs-kafka",
    brokers: ["soulaimane:9092"], // Ensure this is the correct broker address
  });

  const producer = kafka.producer({
    createPartitioner: Partitioners.LegacyPartitioner, // Use legacy partitioner if needed
  });

  try {
    console.log("Connecting to Kafka...");
    await producer.connect();
    console.log("Connected to Kafka!");

    const result = await producer.send({
      topic: "users-topic",
      messages: [
        {
          value: x,
          partition: Math.floor(Math.random() * 2), // Randomly choose a partition (0 or 1)
        },
      ],
    });

    console.log(`Message sent successfully: ${JSON.stringify(result)}`);
  } catch (error) {
    console.error(`Error sending message: ${error.message}`);
  } finally {
    await producer.disconnect();
    console.log("Disconnected from Kafka");
    process.exit(0);
  }
}

if (!x) {
  console.error(
    "No message provided. Please provide a message as a command line argument."
  );
  process.exit(1);
}

run();
