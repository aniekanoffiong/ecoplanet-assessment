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

  @Column('decimal', { precision: 10, scale: 10 })
  value!: number

  @Column({name: 'company_id', type: 'int'})
  companyId: number

  @Column({name: 'location_id', type: 'int'})
  locationId: number

  @TimeColumn()
  timestamp!: Date
}
