//acquire library
const Kafka = require("kafkajs").Kafka
const msg = process.argv[2];

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
        const producer = kafka.producer();
        console.log("Connecting...")
        await producer.connect()
        console.log("Connected!")

        const partition = msg[0] < "N" ? 0 : 1;

        const result = await producer.send({
            "topic": "Users",
            "messages": [
                {
                    "value": msg,
                    "partition": partition
                }
            ]
        })

        console.log(`Done! Created Successfully! ${JSON.stringify(result)}`)
        await producer.disconnect();

    } catch (excecption) {
        console.error(`Something bad happened ${ex}`);
    }

    finally {
        process.exit(0);
    }
}
