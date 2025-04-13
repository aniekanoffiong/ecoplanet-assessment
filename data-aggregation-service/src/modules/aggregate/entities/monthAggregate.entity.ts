import { AggregateColumn, BucketColumn, ContinuousAggregate } from "@timescaledb/typeorm";
import { ConsumptionAggregate } from "./aggregate.entity";
import { Column } from "typeorm";
import { AggregateType } from '@timescaledb/schemas'

@ContinuousAggregate(ConsumptionAggregate, {
  name: 'month_aggregate',
  bucket_interval: '1 hour',
  refresh_policy: {
    start_offset: '3 days',
    end_offset: '1 hour',
    schedule_interval: '1 hour',
  },
})
export class MonthAggregate {
  @BucketColumn({
    source_column: 'timestamp',
  })
  bucket!: Date;

  @AggregateColumn({
    type: AggregateType.Sum,
    column: 'value'
  })
  consumption!: number;

  @Column({
    type: 'int',
    name: 'company_id'
  })
  companyId!: number;

  @Column({
    type: 'int',
    name: 'location_id'
  })
  locationId!: number;
}