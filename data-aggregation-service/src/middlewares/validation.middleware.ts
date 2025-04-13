import { ClassConstructor, plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';
import { NextFunction, Request, Response } from 'express';
import ValidationException from '../exceptions/validation.exception';

const validationMiddleware = (schema: ClassConstructor<object>) => {
  return async (req: Request, _res: Response, next: NextFunction) => {
    const transformedClass: object = plainToInstance(schema, { ...req.body });
    const errors = await validate(transformedClass);
    if (errors.length > 0) {
      next(new ValidationException(errors.map((c) => Object.values(c.constraints || {}).join(', ')).join(', ')));
    }
    next();
  };
};

export default validationMiddleware;
