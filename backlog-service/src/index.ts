import 'reflect-metadata';
import dotenv from 'dotenv';
import { AppDataSource } from './db/data-source';
import BacklogService from './modules/backlog/backlog.service';
import cron from 'node-cron';

dotenv.config();
const minuteInterval: number = 5

AppDataSource.initialize()
  .then(() => console.log('Typeorm successfully initialized'))
  .catch((error) => console.log(`unable to initialize typeorm postgres ${error}`));

cron.schedule(`${minuteInterval} * * * *`, () => {
  console.log(`running the backlog task every ${minuteInterval} minutes`);
  BacklogService.checkForUnprocessedMessageAndWriteToQueue();
});
