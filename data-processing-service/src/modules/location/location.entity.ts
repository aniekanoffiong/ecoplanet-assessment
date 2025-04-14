import { Entity, PrimaryGeneratedColumn, Column, ManyToOne, JoinColumn, OneToMany } from "typeorm"
import { Company } from "../company/company.entity";
import { Device } from "../device/device.entity";

@Entity()
export class Location {
  @PrimaryGeneratedColumn()
  id: number

  @Column('varchar')
  name: string

  @ManyToOne(() => Company)
  @JoinColumn({ name: "company_id" })
  company: Company

  @OneToMany(() => Device, (device) => device.location)
  devices: Device[]
}
