import { Hypertable, TimeColumn } from '@timescaledb/typeorm';
import { Column, Entity, PrimaryColumn } from 'typeorm';

@Hypertable({
  compression: {
    compress: true,
    compress_orderby: 'timestamp',
    compress_segmentby: 'device_id',
    policy: {
      schedule_interval: '1 week',
    },
  },
})
@Entity('consumption_aggregates')
export class ConsumptionAggregate {
  @PrimaryColumn({name: "device_id", type: 'int'})
  deviceId!: number

  @PrimaryColumn({name: 'company_id', type: 'int'})
  companyId: number

  @PrimaryColumn({name: 'location_id', type: 'int'})
  locationId: number

  @Column('decimal', { precision: 10, scale: 10 })
  value!: number

  @TimeColumn()
  timestamp!: Date
}
