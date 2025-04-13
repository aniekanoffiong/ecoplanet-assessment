import { IsDateString, IsNotEmpty, IsNumber } from 'class-validator';

class IngestDTO {
  @IsNumber()
  @IsNotEmpty()
  deviceId: number;

  @IsNumber()
  @IsNotEmpty()
  value: number;

  @IsDateString()
  @IsNotEmpty()
  timestamp: string;
}

export default IngestDTO;
