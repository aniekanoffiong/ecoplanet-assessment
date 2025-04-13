import { IsDateString, IsNumber, IsOptional } from 'class-validator';

class AggregateQueryDTO {
  @IsOptional()
  @IsNumber()
  companyId?: number | undefined;

  @IsOptional()
  @IsNumber()
  locationId?: number | undefined;

  @IsOptional()
  @IsDateString()
  start?: string | undefined;

  @IsOptional()
  @IsDateString()
  end?: string | undefined;
}

export default AggregateQueryDTO;
