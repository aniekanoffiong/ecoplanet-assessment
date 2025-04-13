import IngestDTO from './ingest.dto';
import { Kafka } from 'kafkajs'
import dotenv from 'dotenv';

dotenv.config();

const IngestService = {
  writeToQueue: async function (data: IngestDTO): Promise<void> {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'ingestion-service', 
      brokers,
    });
    const producer = client.producer();
    await producer.connect();
    const topic = process.env.KAFKA_INGESTION_TOPIC as string;
    const messages = [{ value: JSON.stringify(data) }]
    producer.send({
      topic, messages
    });
  },
};

export default IngestService;
