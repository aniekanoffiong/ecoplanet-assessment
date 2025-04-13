import { TimescaleDataSource } from "../../../db/timescale-data-source";
import { ConsumptionAggregate } from "../entities/aggregate.entity";

export default TimescaleDataSource.getRepository(ConsumptionAggregate);