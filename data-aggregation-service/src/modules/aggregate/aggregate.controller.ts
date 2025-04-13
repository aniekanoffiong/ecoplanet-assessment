import { NextFunction, Request, Response } from 'express';
import AggregateService from './aggregate.service';
import { jsonResponse } from '../../utils/jsonResponse';

const AggregateController = {
  aggregates: async function (req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      const aggregateData = AggregateService.retrieveAggregates(req.body);
      jsonResponse(res, 'Aggregates retrieved', aggregateData);
    } catch (err) {
      next(err);
    }
  },
};

export default AggregateController;
