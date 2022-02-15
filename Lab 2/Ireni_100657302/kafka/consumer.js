//acquire library
const Kafka = require("kafkajs").Kafka

run();

async function run() {

    try {
        //create an admin connection to create a topic
        //establish tcp connection

        const kafka = new Kafka({
            "clientId": "myapp",
            "brokers": ["localhost:9093"]//,"localhost:9094","localhost:9095"]  
        })

        //admin interface
        const consumer = kafka.consumer({ "groupId": "test" });
        console.log("Connecting...")
        await consumer.connect()
        console.log("Connected!")

        //does long polling
        // await consumer.disconnect();

        consumer.subscribe({
            "topic": "Users",
            "fromBeginning": true
        })

        await consumer.run({
            "eachMessage": async result => {
                console.log(`Received msg ${result.message.value} on parition ${result.partition}`)
            }
        })

    } catch (excecption) {
        console.error(`Something bad happened ${ex}`);
    }

    finally {
    }
}
