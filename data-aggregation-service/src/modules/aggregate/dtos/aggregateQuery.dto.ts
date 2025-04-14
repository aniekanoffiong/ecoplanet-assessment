import { IsDateString, IsNotEmpty, IsNumber, IsOptional } from 'class-validator';

class AggregateQueryDTO {
  @IsNotEmpty()
  @IsDateString()
  start: string;

  @IsNotEmpty()
  @IsDateString()
  end: string;

  @IsOptional()
  @IsNumber()
  companyId?: number | undefined;

  @IsOptional()
  @IsNumber()
  locationId?: number | undefined;
}

export default AggregateQueryDTO;
