const kafka = require('kafka-node');
const Client = new kafka.KafkaClient({
    kafkaHost: 'localhost:9092'
});

const producer = new kafka.Producer(Client);
const consumer = new kafka.Consumer(
    Client,
    [{ topic: 'hello-world', partition: 0 }],
    { autoCommit: true }
);

producer.on('ready', () => {
    producer.send(
        [{ topic: 'hello-world', messages: ['Hello World!'] }],
        () => console.log('Message Sent')
    );
});

consumer.on('message', (message) => 
    console.log('Received:', message.value)
);
