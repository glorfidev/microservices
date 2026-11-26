package entity

import com.typesafe.config.ConfigFactory
import entity.DatabaseMicroService.*
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.serialization.SerializationExtension
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuiteLike

class DatabaseMicroServiceSpec
    extends TestKit(ActorSystem(
      "DatabaseMicroServiceSpec",
      ConfigFactory.parseString("pekko.actor.provider = local").withFallback(ConfigFactory.load())
    ))
    with AnyFunSuiteLike
    with BeforeAndAfterAll {

  private val database = system.actorOf(Props(new DatabaseMicroService("database-under-test", Set.empty)))

  override protected def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  test("initialization provides stable fictional employer records") {
    Database.init()
    val client = TestProbe()

    client.send(database, FindEmployer("2"))
    client.expectMsg(EmployerMessage(Some(Employer("2", "Harbor Demo Analytics", 12))))
  }

  test("database actor saves, updates, and deletes an employer") {
    val client = TestProbe()
    val employer = Employer("test-organization", "Summit Demo Research", 7)

    client.send(database, SaveEmployer(employer))
    client.send(database, FindEmployer(employer.publicId))
    client.expectMsg(EmployerMessage(Some(employer)))

    client.send(database, UpdateEmployer(employer.publicId, employer.name, 8))
    client.send(database, FindEmployer(employer.publicId))
    client.expectMsg(EmployerMessage(Some(employer.copy(yearsOperating = 8))))

    client.send(database, DeleteEmployer(employer.publicId))
    client.send(database, FindEmployer(employer.publicId))
    client.expectMsg(EmployerMessage(None))
  }

  test("found, missing, and unavailable replies round-trip through Jackson CBOR") {
    val serialization = SerializationExtension(system)
    val replies = List(
      EmployerMessage(Some(Employer("test-cbor", "Cedar Demo Systems", 6))),
      EmployerMessage(None),
      EmployerLookupUnavailable
    )

    replies.foreach { reply =>
      val serializer = serialization.findSerializerFor(reply)
      assert(serializer.getClass.getSimpleName == "JacksonCborSerializer")

      val bytes = serialization.serialize(reply).get
      assert(serialization.deserialize(bytes, reply.getClass).get == reply)
    }
  }
}
