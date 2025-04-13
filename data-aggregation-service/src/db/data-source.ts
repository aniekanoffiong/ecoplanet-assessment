import "reflect-metadata";
import dotenv from "dotenv";
import { DataSource } from "typeorm";
import { ConsumptionLog } from "../modules/consumptionLog/consumptionLog.entity";
import { Company } from "../modules/company/company.entity";
import { Device } from "../modules/device/device.entity";
import { Location } from "../modules/location/location.entity";

dotenv.config();

const AppDataSource = new DataSource({
    type: "postgres",
    host: process.env.DATABASE_HOST,
    port: Number(process.env.DATABASE_PORT),
    username: process.env.DATABASE_USER,
    password: process.env.DATABASE_PASS,
    database: process.env.DATABASE_NAME,
    synchronize: true,
    logging: false,
    entities: [ConsumptionLog, Company, Location, Device],
    migrations: [],
})

export { AppDataSource }
