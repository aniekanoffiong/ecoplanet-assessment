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
          let data: ConsumptionLogDTO = JSON.parse(message.value.toString());
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

  _saveDataInTimeSeriesDB: async function(data: ConsumptionLogDTO) {
    const aggregate: ConsumptionAggregate = await aggregateRepository.create(data);
    aggregateRepository.save(aggregate);
  },
};

export default AggregateService;
