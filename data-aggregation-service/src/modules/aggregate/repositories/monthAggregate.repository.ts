import { TimescaleDataSource } from "../../../db/timescale-data-source";
import { MonthAggregate } from "../entities/monthAggregate.entity";

export default TimescaleDataSource.getRepository(MonthAggregate);
