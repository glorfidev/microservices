package jobdemo

import com.typesafe.config.{ConfigFactory, ConfigValueFactory}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*
import scala.util.Using

class JobWorkflowSpec extends AnyFunSuite {
  import JobWorkflow.*

  test("pending submissions recover from disk and receive one durable decision after restart") {
    val data = Files.createTempDirectory("job-workflow-spec-")
    val command = Submit(UUID.randomUUID().toString, "1", "Demo Warehouse Technician", 2)
    val rejection = Submit(UUID.randomUUID().toString, "2", "Demo Platform Engineer", 8)
    try {
      withSystem(data) { system =>
        val client = TestProbe()(using system)
        val workflow = system.actorOf(props(_ => Future.failed(new IllegalStateException("Review is offline")), Duration.Zero))
        client.send(workflow, command)
        val submitted = client.expectMsgType[Submission]
        assert(submitted.created && submitted.posting.status == "Pending")

        client.send(workflow, command)
        client.expectMsg(Submission(submitted.posting, created = false))
        client.send(workflow, command.copy(positions = 3))
        client.expectMsgType[SubmissionConflict]
        client.send(workflow, rejection)
        assert(client.expectMsgType[Submission].posting.status == "Pending")
      }

      val decided = withSystem(data) { system =>
        val client = TestProbe()(using system)
        val reviewer = system.actorOf(reviewProps)
        val workflow = system.actorOf(props(_ => Future.successful(reviewer), Duration.Zero))
        client.awaitAssert({
          client.send(workflow, ListPostings)
          val byId = client.expectMsgType[List[Posting]].map(posting => posting.id -> posting).toMap
          assert(byId(command.id).status == "Approved")
          assert(byId(rejection.id).status == "Rejected")
          byId
        }, 8.seconds)
      }

      withSystem(data) { system =>
        val client = TestProbe()(using system)
        val reviewer = TestProbe()(using system)
        val workflow = system.actorOf(props(_ => Future.successful(reviewer.ref), Duration.Zero))
        client.send(workflow, ListPostings)
        assert(client.expectMsgType[List[Posting]].map(posting => posting.id -> posting).toMap == decided)
        client.send(workflow, command)
        client.expectMsg(Submission(decided(command.id), created = false))
        reviewer.expectNoMessage(900.millis)
      }
    } finally {
      Using.resource(Files.walk(data)) { paths =>
        paths.iterator().asScala.toList.sortBy(path => -path.getNameCount).foreach(Files.deleteIfExists)
      }
    }
  }

  private def withSystem[A](data: Path)(run: ActorSystem => A): A = {
    val config = ConfigFactory.parseString("""
      pekko.actor.provider = local
      pekko.persistence.journal.plugin = "pekko.persistence.journal.leveldb"
      pekko.persistence.journal.leveldb.native = off
    """).withValue(
      "pekko.persistence.journal.leveldb.dir",
      ConfigValueFactory.fromAnyRef(data.resolve("journal").toString)
    ).withFallback(ConfigFactory.load())
    val system = ActorSystem("JobWorkflowSpec", config)
    try run(system)
    finally TestKit.shutdownActorSystem(system)
  }
}
