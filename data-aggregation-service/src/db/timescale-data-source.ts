import '@timescaledb/typeorm';

import "reflect-metadata";
import dotenv from "dotenv";
import { DataSource } from "typeorm";
import { ConsumptionAggregate } from '../modules/aggregate/entities/aggregate.entity';
import { MonthAggregate } from '../modules/aggregate/entities/monthAggregate.entity';
import { HourlyAggregate } from '../modules/aggregate/entities/hourlyAggregate.entity';

dotenv.config();

const TimescaleDataSource = new DataSource({
  type: "postgres",
  host: process.env.TIMESCALE_HOST,
  port: Number(process.env.TIMESCALE_PORT),
  username: process.env.TIMESCALE_USER,
  password: process.env.TIMESCALE_PASS,
  database: process.env.TIMESCALE_NAME,
  synchronize: true,
  logging: false,
  entities: [ConsumptionAggregate, MonthAggregate, HourlyAggregate],
  migrations: [],
})

export { TimescaleDataSource }
