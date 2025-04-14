import { ConsumptionLogDTO } from './consumptionLog.dto';
import dotenv from 'dotenv';
import consumptionLogRepository from '../consumptionLog/consumptionLog.repository';
import { ConsumptionLog } from '../consumptionLog/consumptionLog.entity';
import { Kafka } from 'kafkajs';

dotenv.config();

const BacklogService = {
  checkForUnprocessedMessageAndWriteToQueue: async function(): Promise<void> {
    try {
      const messages = await BacklogService._retrieveUnprocessedMessage()
      console.log(`retrieved messages for backlog processing --- `, messages.length)
      if (messages.length > 0) {
        BacklogService._writeToQueue(messages.map(({ id, ...data}) => data))
        BacklogService._updateMessagesAsProcessed(messages)
      }
    } catch (err) {
      console.error(`Failed to complete backlog action --- `, err)
    }
  },

  _retrieveUnprocessedMessage: async function(): Promise<ConsumptionLogDTO[]> {
    return await consumptionLogRepository
      .createQueryBuilder('consumptionLog')
      .innerJoin('consumptionLog.device', 'device')
      .innerJoin('device.location', 'location')
      .select('consumptionLog.id', 'id')
      .addSelect('consumptionLog.device_id', 'deviceId')
      .addSelect('consumptionLog.value', 'value')
      .addSelect('consumptionLog.timestamp', 'timestamp')
      .addSelect('location.company_id', 'companyId')
      .addSelect('location.id', 'locationId')
      .where('consumptionLog.isProcessed = :processedStatus', { processedStatus: false })
      .orderBy('consumptionLog.timestamp', 'DESC')
      .limit(100)
      .getRawMany()
  },

  _writeToQueue: async function (data: Omit<ConsumptionLogDTO, 'id'>[]): Promise<void> {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-aggregate-service', 
      brokers,
    });
    const producer = client.producer();
    await producer.connect();
    const topic = process.env.KAFKA_BACKLOG_TOPIC as string;
    const messages = [{ value: JSON.stringify(data) }]
    producer.send({
      topic, messages
    });
  },

  _updateMessagesAsProcessed: async function(messages: ConsumptionLogDTO[]): Promise<void> {
    const messageIds = messages.map(it => it.id)
    console.info(`Attempting to update messages with id --- `, messageIds)
    if (messageIds.length) {
      consumptionLogRepository
        .createQueryBuilder('consumptionLog')
        .update(ConsumptionLog)
        .set({ isProcessed: true })
        .whereInIds(messageIds)
        .execute()
    }
  }
};

export default BacklogService;
