const { Kafka } = require("kafkajs");

async function run() {
  try {
    const kafka = new Kafka({
      // app name
      clientId: "nodejs-kafka",
      // server brokers
      brokers: ["soulaimane:9092"],
    });

    const admin = kafka.admin();
    console.log("connecting ...");

    await admin.connect();
    console.log("connect ...");

    await admin.createTopics({
      topics: [
        {
          topic: "users-topic",
          numPartitions: 2,
        },
      ],
    });
    console.log("topics created !");

    await admin.disconnect();
  } catch (error) {
    console.log(`error ! ${error}`);
  } finally {
    process.exit(0);
  }
}

run();
