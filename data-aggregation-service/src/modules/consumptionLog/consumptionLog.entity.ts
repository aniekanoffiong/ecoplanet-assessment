import { Entity, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn } from "typeorm"
import { Device } from "../device/device.entity"

@Entity()
export class ConsumptionLog {
  @PrimaryGeneratedColumn()
  id: number

  @ManyToOne(() => Device)
  @JoinColumn({ name: "device_id" })
  device: Device

  @Column('decimal', { precision: 10, scale: 10 })
  value: number

  @Column({name: 'timestamp', type: 'timestamptz'})
  timestamp: Date

  @Column({name: 'is_processed', type: 'boolean'})
  isProcessed: Boolean = false;
}
