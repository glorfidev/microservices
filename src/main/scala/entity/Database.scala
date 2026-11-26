package entity

import microservices.CborSerializable
import scala.collection.concurrent.TrieMap

object Database {

  def init(): Unit = DemoEmployers.records.foreach(Db.save)
}

case class Employer(publicId: String, name: String, yearsOperating: Int) extends CborSerializable

/** Fictional organizations used by the runnable example. */
object DemoEmployers {
  val DefaultId = "1"

  val records = List(
    Employer(DefaultId, "Northstar Demo Robotics", 24),
    Employer("2", "Harbor Demo Analytics", 12),
    Employer("3", "Meadow Demo Labs", 4)
  )
}

object Db {
  private val employers = TrieMap.empty[String, Employer]

  def save(employer: Employer): Employer = {
    employers.put(employer.publicId, employer)
    employer
  }

  def find(publicId: String): Option[Employer] =
    employers.get(publicId)

  def delete(publicId: String): Option[Employer] =
    employers.remove(publicId)
}
