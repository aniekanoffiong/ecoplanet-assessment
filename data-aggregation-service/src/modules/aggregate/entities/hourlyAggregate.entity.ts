import { AggregateColumn, BucketColumn, ContinuousAggregate } from "@timescaledb/typeorm";
import { ConsumptionAggregate } from "./aggregate.entity";
import { AggregateType } from '@timescaledb/schemas'

@ContinuousAggregate(ConsumptionAggregate, {
  name: 'month_aggregate',
  bucket_interval: '1 hour',
  refresh_policy: {
    start_offset: '1 day',
    end_offset: '1 hour',
    schedule_interval: '30 minute',
  },
})
export class HourlyAggregate {
  @BucketColumn({
    source_column: 'timestamp',
  })
  bucket!: Date;

  @AggregateColumn({
    type: AggregateType.Sum,
    column: 'value'
  })
  consumption!: number;

  @AggregateColumn({
    type: AggregateType.Bucket,
    column: 'company_id'
  })
  company_id!: number;

  @AggregateColumn({
    type: AggregateType.Bucket,
    column: 'location_id'
  })
  location_id!: number;
}