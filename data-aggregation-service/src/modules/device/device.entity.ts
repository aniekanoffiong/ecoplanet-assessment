import { Entity, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, OneToMany } from "typeorm"
import { Location } from "../location/location.entity"
import { ConsumptionLog } from "../consumptionLog/consumptionLog.entity"

@Entity()
export class Device {
  @PrimaryGeneratedColumn()
  id: number

  @Column({name: "title", type: 'varchar'})
  title: String

  @ManyToOne(() => Location)
  @JoinColumn({ name: "location_id" })
  location: Location

  @OneToMany(() => ConsumptionLog, (consumptionLog) => consumptionLog.device)
  consumptionLogs: ConsumptionLog[]
}
