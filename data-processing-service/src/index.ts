import 'reflect-metadata';
import dotenv from 'dotenv';
import { AppDataSource } from './db/data-source';
import ProcessingService from './modules/processing/processing.service';

dotenv.config();

AppDataSource.initialize()
  .then(() => console.log('Typeorm successfully initialized'))
  .catch((error) => console.log(`unable to initialize typeorm postgres ${error}`, error));

ProcessingService.pullFromQueueAndSave();
