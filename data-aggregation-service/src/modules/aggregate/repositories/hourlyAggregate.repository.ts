import { TimescaleDataSource } from "../../../db/timescale-data-source";
import { HourlyAggregate } from "../entities/hourlyAggregate.entity";

export default TimescaleDataSource.getRepository(HourlyAggregate);
