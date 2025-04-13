import 'reflect-metadata';
import app from './app'
import http, { Server } from 'http';
import { HttpError } from 'http-errors';
import dotenv from 'dotenv';
import AggregateService from './modules/aggregate/aggregate.service';
import { TimescaleDataSource } from './db/timescale-data-source';

dotenv.config();

TimescaleDataSource.initialize()
  .then(() => console.log('Typeorm successfully initialized for timescale DB'))
  .catch((error) => console.log(`unable to initialize typeorm timescaledb ${error}`, error));

AggregateService.pullFromQueueAndWriteToTimeseriesStorage();
AggregateService.pullFromBacklogQueueAndWriteToTimeseriesStorage();

const PORT = process.env.APP_PORT;
const server: Server = http.createServer(app);

// Start server
server.listen(PORT);
server.on('error', onError);
server.on('listening', onListening);

function onError(error: HttpError) {
  console.log('catching error', error);
  if (error.syscall !== 'listen') {
    throw error;
  }

  // handle specific listen errors with friendly messages
  switch (error.code) {
    case 'EACCES':
      console.error(PORT + ' requires elevated privileges');
      process.exit(1);
      break;
    case 'EADDRINUSE':
      console.error(PORT + ' is already in use');
      process.exit(1);
      break;
    default:
      throw error;
  }
}

function onListening() {
  const addr = server.address();
  console.log('Listening on ' + JSON.stringify(addr));
}
