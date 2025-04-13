import 'reflect-metadata';
import dotenv from 'dotenv';
import cron from 'node-cron';
import ExternalMeterService from './modules/externalMeter/externalMeter.service';

dotenv.config();

const secondInterval: number = 10

console.log(`Starting external meter platform --- `, new Date().toISOString())
cron.schedule(`${secondInterval} * * * * *`, () => {
  console.log(`running the external meter task every ${secondInterval} seconds`, new Date().toISOString());
  ExternalMeterService.trigger();
});
