import { Entity, PrimaryGeneratedColumn, Column, OneToMany } from "typeorm"
import { Location } from "../location/location.entity"

@Entity()
export class Company {
  @PrimaryGeneratedColumn()
  id: number

  @Column("varchar")
  name: String

  @OneToMany(() => Location, (location) => location.company)
  locations: Location[]
}
