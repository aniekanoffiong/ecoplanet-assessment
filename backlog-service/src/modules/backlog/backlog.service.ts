import { ConsumptionLogDTO } from './consumptionLog.dto';
import dotenv from 'dotenv';
import consumptionLogRepository from '../consumptionLog/consumptionLog.repository';
import { ConsumptionLog } from '../consumptionLog/consumptionLog.entity';
import { Kafka } from 'kafkajs';

dotenv.config();

const BacklogService = {
  checkForUnprocessedMessageAndWriteToQueue: async function(): Promise<void> {
    const messages = await BacklogService._retrieveUnprocessedMessage()
    if (messages.length > 0) {
      messages.forEach(BacklogService._writeToQueue)
    }
    BacklogService._updateMessagesAsProcessed(messages)
  },

  _retrieveUnprocessedMessage: async function(): Promise<ConsumptionLogDTO[]> {
    return await consumptionLogRepository
      .createQueryBuilder('consumptionLog')
      .innerJoin('consumptionLog.device', 'device')
      .innerJoin('device.location', 'location')
      .addSelect('location.company_id', 'companyId')
      .addSelect('location.id', 'locationId')
      .where('consumptionLog.isProcessed = :processedStatus', { processedStatus: false })
      .limit(100)
      .getRawMany()
  },

  _writeToQueue: async function (data: ConsumptionLogDTO): Promise<void> {
    const { id, ...finalData } = data
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-aggregate-service', 
      brokers,
    });
    const producer = client.producer();
    await producer.connect();
    const topic = process.env.KAFKA_BACKLOG_TOPIC as string;
    const messages = [{ value: JSON.stringify(finalData) }]
    producer.send({
      topic, messages
    });
  },

  _updateMessagesAsProcessed: async function(messages: ConsumptionLogDTO[]): Promise<void> {
    consumptionLogRepository
      .createQueryBuilder('consumptionLog')
      .update(ConsumptionLog)
      .set({ isProcessed: true })
      .whereInIds(messages.map(it => it.id))
  }
};

export default BacklogService;
