import IngestDTO from './dtos/ingest.dto';
import dotenv from 'dotenv';
import { validate } from 'class-validator';
import { plainToInstance } from 'class-transformer';
import consumptionLogRepository from '../consumptionLog/consumptionLog.repository';
import AggregateQueryDTO from './dtos/aggregateQuery.dto';
import aggregateRepository from './repositories/aggregate.repository';
import { ConsumptionAggregate } from './entities/aggregate.entity';
import { CompanyLocationDTO } from './dtos/companyLocation.dto';
import { createClient } from 'redis';
import monthAggregateRepository from './repositories/monthAggregate.repository';  
import { MonthAggregate } from './entities/monthAggregate.entity';
import { ConsumptionLogDTO } from './dtos/consumptionLog.dto';
import { Kafka, KafkaMessage } from 'kafkajs';

dotenv.config();

const AggregateService = {
  pullFromQueueAndWriteToTimeseriesStorage: async function (): Promise<void> {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-aggregate-service', 
      brokers,
    });
    const topic = process.env.KAFKA_INGESTION_TOPIC as string;
    const consumer = client.consumer({ groupId: 'data-aggregate-consumer' });
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
        console.log('retrieved ingest message for timeseries', message);
        if (message.value) {
          let data: IngestDTO = JSON.parse(message.value.toString());
          if (await AggregateService._validateMessage(data)) {
            await AggregateService._persistToTimeSeriesDB(data);
          }
        }
      },
    })
  },

  pullFromBacklogQueueAndWriteToTimeseriesStorage: async function (): Promise<void> {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-aggregate-service', 
      brokers,
    });
    const topic = process.env.KAFKA_BACKLOG_TOPIC as string;
    const consumer = client.consumer({ groupId: 'data-aggregate-consumer' });
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
        console.log('retrieved backlog message', message);
        if (message.value) {
          let data: ConsumptionLogDTO = JSON.parse(message.value.toString());
          AggregateService._saveDataInTimeSeriesDB(data)
        }
      },
    })    
  },

  retrieveAggregates: async function (data: AggregateQueryDTO): Promise<MonthAggregate[]> {
    let queryBuilder = monthAggregateRepository
      .createQueryBuilder()
      .where('start >= :start', { start: data.start })
      .andWhere('end >= :end', { end: data.end });
    if (data.companyId) {
      queryBuilder = queryBuilder.andWhere('companyId = :companyId', { companyId: data.companyId })
    }
    if (data.locationId) {
      queryBuilder = queryBuilder.andWhere('locationId = :locationId', { locationId: data.locationId })
    }
    return queryBuilder.getMany();
  },

  _validateMessage: async function(data: IngestDTO): Promise<boolean> {
    const transformedClass: object = plainToInstance(IngestDTO, { data });
    const errors = await validate(transformedClass);
    if (errors.length > 0) {
      return false;
    }
    return true;
  },

  _persistToTimeSeriesDB: async function(data: IngestDTO) {
    const companyAndLocation = await AggregateService._fetchCompanyAndLocation(data.deviceId);
    if (!companyAndLocation) {
      await AggregateService._writeToReviewQueue(data);
      return;
    }
    AggregateService._saveDataInTimeSeriesDB({
      ...data,
      locationId: companyAndLocation.locationId,
      companyId: companyAndLocation.companyId,
    })
  },

  _saveDataInTimeSeriesDB: async function(data: ConsumptionLogDTO) {
    const aggregate: ConsumptionAggregate = await aggregateRepository.create(data);
    aggregateRepository.save(aggregate);
  },

  _fetchCompanyAndLocation: async function (deviceId: number): Promise<CompanyLocationDTO | null> {
    const cacheClient = await AggregateService._getRedisClient()
    const cachedData = await cacheClient.get(deviceId.toString())
    if (cachedData) {
      return JSON.parse(cachedData)
    }
    const dataFromDB = await AggregateService._fetchDataFromDatabase(deviceId)
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
  }
};

export default AggregateService;
