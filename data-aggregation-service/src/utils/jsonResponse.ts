import { Response } from 'express';

export const jsonResponse = (res: Response, message: string = "", data: object | null = null, statusCode: number = 200) => {
  return res.status(statusCode).json({
    status: 'success',
    message,
    data,
  });
};

export const errorResponse = (message: string, data: object, res: Response, statusCode: number = 400) => {
  return res.status(statusCode).json({
    status: 'error',
    message,
    data,
  });
};
