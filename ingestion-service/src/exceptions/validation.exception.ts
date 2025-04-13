import HttpException from './http.exception';

class ValidationException extends HttpException {
  constructor(message: string) {
    super(422, message);
  }
}

export default ValidationException;
