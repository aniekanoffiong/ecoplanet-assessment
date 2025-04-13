import IngestDTO from './ingest.dto';
import kafka, { Message } from 'kafka-node'
import dotenv from 'dotenv';
import { validate } from 'class-validator';
import { plainToInstance } from 'class-transformer';
import { ConsumptionLog } from '../consumptionLog/consumptionLog.entity';
import consumptionLogRepository from '../consumptionLog/consumptionLog.repository';

dotenv.config();

const IngestService = {
  pullFromQueueAndSave: async function (): Promise<void> {
    const client = new kafka.KafkaClient({ kafkaHost: process.env.KAFKA_HOST });
    const topic = process.env.KAFKA_INGESTION_TOPIC as string;
    const consumer = new kafka.Consumer(
      client,
      [{ topic, partition: 0 }],
      { autoCommit: false }
    );
    consumer.on('message', async function (message: Message) {
      console.log(message);
      let data: IngestDTO = JSON.parse(message.value.toString());
      if (await IngestService.validateMessage(data)) {
        await IngestService.persistMessage(data);
      }
    });
   
    consumer.on('error', function (err) {
      console.error(`Error in Queue`, err)
    })
  },

  validateMessage: async function(data: IngestDTO): Promise<boolean> {
    const transformedClass: object = plainToInstance(IngestDTO, { data });
    const errors = await validate(transformedClass);
    if (errors.length > 0) {
      return false;
    }
    return true;
  },

  persistMessage: async function(data: IngestDTO) {
    const consumptionLog: ConsumptionLog = await consumptionLogRepository.create(data);
    consumptionLogRepository.save(consumptionLog);
  }
};

export default IngestService;
