import IngestDTO from './dtos/ingest.dto';
import { Kafka, KafkaMessage } from 'kafkajs'
import dotenv from 'dotenv';
import { validate } from 'class-validator';
import { plainToInstance } from 'class-transformer';
import { ConsumptionLog } from '../consumptionLog/consumptionLog.entity';
import consumptionLogRepository from '../consumptionLog/consumptionLog.repository';
import { createClient } from 'redis';
import { CompanyLocationDTO } from './dtos/companyLocation.dto';
import { ConsumptionLogDTO } from './dtos/consumptionLog.dto';

dotenv.config();

const ProcessingService = {
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
        if (message.value) {
          let data: IngestDTO = JSON.parse(message.value.toString());
          const validateMessage = await ProcessingService._validateMessage(data)
          console.log(`content of data retrieved and validate --- `, data, validateMessage)
          if (validateMessage) {
            await ProcessingService._processForAggregateQueue(data)
            await ProcessingService._persistToDB(data);
          }
        }
      },
    })    
  },

  _validateMessage: async function(data: IngestDTO): Promise<boolean> {
    const transformedClass: object = plainToInstance(IngestDTO, data);
    const errors = await validate(transformedClass);
    if (errors.length > 0) {
      return false;
    }
    return true;
  },

  _processForAggregateQueue: async function(data: IngestDTO) {
    const companyAndLocation = await ProcessingService._fetchCompanyAndLocation(data.deviceId);
    if (!companyAndLocation) {
      console.info(`Unable to find Company & Location for record ---`, data)
      await ProcessingService._writeToReviewQueue(data);
      return;
    }
    const finalData = {
      ...data,
      locationId: companyAndLocation.locationId,
      companyId: companyAndLocation.companyId,
    }
    ProcessingService._writeToAggregateQueue(finalData)
  },

  _writeToReviewQueue: async function(data: IngestDTO) {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-aggregate-service', 
      brokers,
    });
    const producer = client.producer();
    await producer.connect();
    const topic = process.env.KAFKA_REVIEW_TOPIC as string;
    const messages = [{ value: JSON.stringify(data) }]
    producer.send({
      topic, messages
    });
  },

  _writeToAggregateQueue: async function(data: ConsumptionLogDTO) {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-processing-service', 
      brokers,
    });
    const producer = client.producer();
    await producer.connect();
    const topic = process.env.KAFKA_AGGREGATE_TOPIC as string;
    const messages = [{ value: JSON.stringify(data) }]
    producer.send({
      topic, messages
    });
  },

  _fetchCompanyAndLocation: async function (deviceId: number): Promise<CompanyLocationDTO | null> {
    const cacheClient = await ProcessingService._getRedisClient()
    const cachedData = await cacheClient.get(deviceId.toString())
    if (cachedData) {
      return JSON.parse(cachedData)
    }
    const dataFromDB = await ProcessingService._fetchDataFromDatabase(deviceId)
    console.log(`data from database Query --- `, dataFromDB)
    if (dataFromDB) {
      cacheClient.set(deviceId.toString(), JSON.stringify(dataFromDB))
      return dataFromDB
    }
    return null
  },

  _getRedisClient: async function() {
    return await createClient({
        socket: {
          host: process.env.REDIS_HOST,
          port: 6379,
        }
      })
      .on('error', err => console.log('Redis Client Error', err))
      .connect();
  },

  _fetchDataFromDatabase: async function (deviceId: number): Promise<CompanyLocationDTO | null | undefined> {
    return await consumptionLogRepository
      .createQueryBuilder("consumptionLog")
      .innerJoin("consumptionLog.device", "device")
      .innerJoin("device.location", "location")
      .select("location.company_id", 'companyId')
      .addSelect("location.id", 'locationId')
      .where("consumptionLog.device_id = :deviceId", { deviceId })
      .getRawOne()
  },

  _persistToDB: async function(data: IngestDTO) {
    const consumptionLogData: ConsumptionLog = await consumptionLogRepository.create(data);
    const consumptionLog = await consumptionLogRepository.save(consumptionLogData);
    console.info(`consumption log has been persisted successfully --- `, consumptionLog);
  }
};

export default ProcessingService;
