import { NextFunction, Request, Response } from 'express';
import IngestService from './ingest.service';
import { jsonResponse } from '../../utils/jsonResponse';

const IngestController = {
  ingest: async function (req: Request, res: Response, next: NextFunction): Promise<void> {
    try {
      jsonResponse(res);
      await IngestService.writeToQueue(req.body);
    } catch (err) {
      next(err);
    }
  },
};

export default IngestController;
