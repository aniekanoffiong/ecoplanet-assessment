import IngestDTO from './dtos/ingest.dto';
import dotenv from 'dotenv';
import { validate } from 'class-validator';
import { plainToInstance } from 'class-transformer';
import AggregateQueryDTO from './dtos/aggregateQuery.dto';
import aggregateRepository from './repositories/aggregate.repository';
import { ConsumptionAggregate } from './entities/aggregate.entity';
import monthAggregateRepository from './repositories/monthAggregate.repository';  
import { MonthAggregate } from './entities/monthAggregate.entity';
import { ConsumptionLogDTO } from './dtos/consumptionLog.dto';
import { Kafka, KafkaMessage } from 'kafkajs';
import hourlyAggregateRepository from './repositories/hourlyAggregate.repository';

dotenv.config();

const AggregateService = {
  pullFromQueueAndWriteToTimeseriesStorage: async function (): Promise<void> {
    const brokers = [process.env.KAFKA_HOST as string]
    const client = new Kafka({
      clientId: 'data-aggregate-service', 
      brokers,
    });
    const topic = process.env.KAFKA_AGGREGATE_TOPIC as string;
    const consumer = client.consumer({ groupId: 'data-aggregate-consumer' });
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({
      eachMessage: async ({ message }: { message: KafkaMessage }) => {
        if (message.value) {
          let data: ConsumptionLogDTO = JSON.parse(message.value.toString());
          console.info(`pulled data from aggregate topic --- `, data)
          if (await AggregateService._validateMessage(data)) {
            await AggregateService._saveDataInTimeSeriesDB(data)
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
        if (message.value) {
          let data: ConsumptionLogDTO = JSON.parse(message.value.toString());
          console.info(`pulled data from backlog topic --- `, data)
          AggregateService._saveDataInTimeSeriesDB(data)
        }
      },
    })    
  },

  retrieveAggregates: async function (data: AggregateQueryDTO): Promise<MonthAggregate[]> {
    try {
      let queryBuilder = hourlyAggregateRepository
        .createQueryBuilder()
        .where('bucket >= :start', { start: data.start })
        .andWhere('bucket <= :end', { end: data.end });
      if (data.companyId) {
        queryBuilder = queryBuilder.andWhere(
          'company_id = :companyId',
          { companyId: data.companyId }
        )
      }
      if (data.locationId) {
        queryBuilder = queryBuilder.andWhere(
          'location_id = :locationId',
          { locationId: data.locationId }
        )
      }
      if (data.companyId) {
        queryBuilder = queryBuilder.groupBy(data.companyId.toString())
      }
      if (data.locationId) {
        queryBuilder = queryBuilder.groupBy(data.locationId.toString())
      }
      const result = await queryBuilder.getRawMany();
      console.log(`successfully retrieved data from aggregates --- `, result)
      return result;
    } catch (err) {
      console.error(`Query failed --- `, err)
    }
    return []
  },

  _validateMessage: async function(data: IngestDTO): Promise<boolean> {
    const transformedClass: object = plainToInstance(IngestDTO, data);
    const errors = await validate(transformedClass);
    if (errors.length > 0) {
      return false;
    }
    return true;
  },

  _saveDataInTimeSeriesDB: async function(data: ConsumptionLogDTO) {
    try {
      const aggregateData: ConsumptionAggregate = await aggregateRepository.create(data);
      const aggregate = await aggregateRepository.save(aggregateData);
      console.log(`successfully persisted aggregate data --- `, aggregate)
    } catch (err) {
      console.log(`Unable to persist data in timeseries`, err)
    }
  },
};

export default AggregateService;
