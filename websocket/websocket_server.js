const WebSocket = require('ws');
const { Kafka } = require('kafkajs');

// Kafka config
const kafka = new Kafka({
  clientId: 'websocket-service',
  brokers: ['10.10.101.13:9092']
});

// Kafka consumer từ topic sentiment_predictions
const consumer = kafka.consumer({ groupId: 'prediction-group' });
// Kafka producer để gửi input vào topic sentiment_data
const producer = kafka.producer();

// WebSocket server
const wss = new WebSocket.Server({ port: 8080 });

wss.on('connection', ws => {
  console.log('🔌 Client connected to WebSocket');

  // Nhận dữ liệu từ client và gửi vào Kafka topic sentiment_data
  ws.on('message', async (message) => {
    console.log(`Received message from client: ${message}`);
    await producer.send({
      topic: 'sentiment_data',
      messages: [{ value: message }]
    });
  });
});

// Hàm lắng nghe dữ liệu dự đoán từ Kafka
const startKafka = async () => {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'sentiment_predictions', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const prediction = message.value.toString();
      console.log(`Sending prediction to WebSocket clients: ${prediction}`);
      // Gửi tới tất cả client đang kết nối
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(prediction);
        }
      });
    }
  });
};

startKafka().catch(console.error);
console.log('🚀 WebSocket + Kafka server running on ws://localhost:8080');
