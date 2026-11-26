package webserver

import entity.DatabaseMicroService.{EmployerLookupUnavailable, EmployerMessage}
import entity.Employer
import microservices.ServiceRegistryExtension.ServiceUnavailable
import org.apache.pekko.http.scaladsl.model.{ContentTypes, StatusCodes}
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

class EmployerRoutesSpec extends AnyFunSuite with ScalatestRouteTest {
  override def testConfigSource: String = "pekko.actor.provider = local"

  private val employers = Map(
    "1" -> Employer("1", "Northstar Demo Robotics", 24),
    "2" -> Employer("2", "Harbor Demo Analytics", 12)
  )

  private def routes(queueDemo: () => Future[Unit] = () => Future.successful(())) =
    EmployerRoutes(id => Future.successful(EmployerMessage(employers.get(id))), queueDemo)

  test("GET /employer returns the default organization") {
    Get("/employer") ~> routes() ~> check {
      assert(status == StatusCodes.OK)
      assert(responseAs[String] == "Northstar Demo Robotics")
    }
  }

  test("GET /employer/{id} returns the requested organization as plain text") {
    Get("/employer/2") ~> routes() ~> check {
      assert(status == StatusCodes.OK)
      assert(contentType == ContentTypes.`text/plain(UTF-8)`)
      assert(responseAs[String] == "Harbor Demo Analytics")
    }
  }

  test("GET /employer/{id} returns 404 for an unknown ID") {
    Get("/employer/unknown") ~> routes() ~> check {
      assert(status == StatusCodes.NotFound)
    }
  }

  test("GET /employer/{id} returns 503 when the lookup times out") {
    val route = EmployerRoutes(_ => Future.failed(new TimeoutException), () => Future.successful(()))

    Get("/employer/2") ~> route ~> check {
      assert(status == StatusCodes.ServiceUnavailable)
      assert(responseAs[String] == "Employer lookup temporarily unavailable")
    }
  }

  test("GET /employer/{id} returns 503 when a dependency is unavailable") {
    val routes = List(
      EmployerRoutes(_ => Future.failed(new ServiceUnavailable("webserverMicroservices")), () => Future.successful(())),
      EmployerRoutes(_ => Future.successful(EmployerLookupUnavailable), () => Future.successful(()))
    )

    routes.foreach { route =>
      Get("/employer/2") ~> route ~> check {
        assert(status == StatusCodes.ServiceUnavailable)
      }
    }
  }

  test("GET /employerToScreen queues one example request") {
    val queued = new AtomicInteger()
    val route = routes(() => {
      queued.incrementAndGet()
      Future.successful(())
    })

    Get("/employerToScreen") ~> route ~> check {
      assert(status == StatusCodes.Accepted)
      assert(queued.get() == 1)
    }
  }
}
