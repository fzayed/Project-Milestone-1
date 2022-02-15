//acquire library
const Kafka = require("kafkajs").Kafka

run();

async function run() {

    try {
        //create an admin connection to create a topic
        //establish tcp connection

        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": ["localhost:9093", "localhost:9094", "localhost:9095"]
        })

        //admin interface
        const admin = kafka.admin();
        console.log("Connecting.....")
        await admin.connect()
        console.log("Connected!")

        //create a topic A-M
        await admin.createTopics({
            "topics": [{
                "topic": "Users",
                "numPartitions": 2
            }]
        })

        console.log("Done! Created Successfully")
        await admin.disconnect();

    } catch (excecption) {
        console.error(`Something bad happened ${ex}`);
    }

    finally {
        process.exit(0);
    }
}
