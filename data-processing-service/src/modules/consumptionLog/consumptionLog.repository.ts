import { AppDataSource } from "../../db/data-source";
import { ConsumptionLog } from "./consumptionLog.entity";

export default AppDataSource.getRepository(ConsumptionLog);