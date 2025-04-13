import 'reflect-metadata';
import express, { Application, NextFunction, Request, Response } from 'express';
import HttpException from './exceptions/http.exception';
import validationMiddleware from './middlewares/validation.middleware';
import IngestDTO from './modules/aggregate/dtos/ingest.dto';
import AggregateController from './modules/aggregate/aggregate.controller';

const app: Application = express();
app.set('etag', false);
app.set('x-powered-by', false);

app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.use('/aggregates', validationMiddleware(IngestDTO), AggregateController.aggregates);

app.use((_req: Request, res: Response): Response => {
  return res.status(404).send({ status: 404, data: null, message: 'Not Found' });
});

// eslint-disable-next-line @typescript-eslint/no-unused-vars
app.use((err: HttpException, req: Request, res: Response, _next: NextFunction): void => {
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};
  const responseStatus = err.status || 500;

  // render the error page
  res.status(responseStatus);
  res.send({
    status: 'error',
    data: null,
    message: responseStatus === 500 ? 'There was an internal error' : err.message,
  });
});

export default app;
