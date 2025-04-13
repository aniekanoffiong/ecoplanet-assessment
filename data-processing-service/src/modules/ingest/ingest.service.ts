import IngestDTO from './ingest.dto';
import { Kafka, KafkaMessage } from 'kafkajs'
import dotenv from 'dotenv';
import { validate } from 'class-validator';
import { plainToInstance } from 'class-transformer';
import { ConsumptionLog } from '../consumptionLog/consumptionLog.entity';
import consumptionLogRepository from '../consumptionLog/consumptionLog.repository';

dotenv.config();

const AggregateService = {
  pullFromQueueAndSave: async function (): Promise<void> {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-processing-service', 
      brokers,
    });
    const topic = process.env.KAFKA_INGESTION_TOPIC as string;
    const consumer = client.consumer({ groupId: 'data-processing-consumer' });
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
        console.log(`kafka ingestion message retrieved `, message);
        if (message.value) {
          let data: IngestDTO = JSON.parse(message.value.toString());
          if (await AggregateService._validateMessage(data)) {
            console.info(`data validated as IngestDTO --- `, data)
            await AggregateService._persistToTimeseriesDB(data);
          }
        }
      },
    })    
  },

  _validateMessage: async function(data: IngestDTO): Promise<boolean> {
    const transformedClass: object = plainToInstance(IngestDTO, { data });
    const errors = await validate(transformedClass);
    if (errors.length > 0) {
      return false;
    }
    return true;
  },

  _persistToTimeseriesDB: async function(data: IngestDTO) {
    const consumptionLogData: ConsumptionLog = await consumptionLogRepository.create(data);
    const consumptionLog = await consumptionLogRepository.save(consumptionLogData);
    console.info(`consumption log has been persisted successfully --- `, consumptionLog);
  }
};

export default AggregateService;
