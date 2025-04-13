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
        console.log(`kafka ingestion message retrieved --- `, message);
        if (message.value) {
          let data: IngestDTO = JSON.parse(message.value.toString());
          if (await ProcessingService._validateMessage(data)) {
            console.info(`data validated as IngestDTO --- `, data)
            await ProcessingService._processForAggregateQueue(data)
            await ProcessingService._persistToDB(data);
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

  _processForAggregateQueue: async function(data: IngestDTO) {
    const companyAndLocation = await ProcessingService._fetchCompanyAndLocation(data.deviceId);
    if (!companyAndLocation) {
      console.info(`Unable to find Company & Location for record ---`, data)
      await ProcessingService._writeToReviewQueue(data);
      return;
    }
    ProcessingService._writeToAggregateQueue({
      ...data,
      locationId: companyAndLocation.locationId,
      companyId: companyAndLocation.companyId,
    })
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
    if (dataFromDB) {
      cacheClient.set(deviceId.toString(), JSON.stringify(dataFromDB))
      return dataFromDB
    }
    return null
  },

  _getRedisClient: async function() {
    return await createClient()
      .on('error', err => console.log('Redis Client Error', err))
      .connect();
  },

  _fetchDataFromDatabase: async function (deviceId: number): Promise<CompanyLocationDTO | null | undefined> {
    return await consumptionLogRepository
      .createQueryBuilder("consumptionLog")
      .innerJoin("consumptionLog.device", "device")
      .innerJoin("device.location", "location")
      .select("location.companyId", 'companyId')
      .addSelect("location.id", 'locationId')
      .where("consumptionLog.deviceId = :deviceId", { deviceId })
      .getRawOne()
  },

  _persistToDB: async function(data: IngestDTO) {
    const consumptionLogData: ConsumptionLog = await consumptionLogRepository.create(data);
    const consumptionLog = await consumptionLogRepository.save(consumptionLogData);
    console.info(`consumption log has been persisted successfully --- `, consumptionLog);
  }
};

export default ProcessingService;
