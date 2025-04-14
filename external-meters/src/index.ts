import 'reflect-metadata';
import dotenv from 'dotenv';
import cron from 'node-cron';
import ExternalMeterService from './modules/externalMeter/externalMeter.service';

dotenv.config();

console.log(`Starting external meter platform --- `, new Date().toISOString())
cron.schedule(`0,20,40 * * * * *`, () => {
  console.log(`running the external meter task every 0,20,40 seconds`, new Date().toISOString());
  ExternalMeterService.trigger();
});
