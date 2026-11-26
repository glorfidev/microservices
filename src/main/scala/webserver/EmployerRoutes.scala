package webserver

import entity.DatabaseMicroService.{EmployerLookupReply, EmployerLookupUnavailable, EmployerMessage}
import entity.DemoEmployers
import microservices.ServiceRegistryExtension.ServiceUnavailable
import org.apache.pekko.http.scaladsl.model.StatusCodes
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route

import java.util.concurrent.TimeoutException
import scala.concurrent.Future
import scala.util.{Failure, Success}

private[webserver] object EmployerRoutes {
  def apply(
      findEmployer: String => Future[EmployerLookupReply],
      queueDemo: () => Future[Unit]
  ): Route = {
    def employerById(id: String): Route =
      onComplete(findEmployer(id)) {
        case Success(EmployerMessage(Some(employer))) => complete(employer.name)
        case Success(EmployerMessage(None)) => complete(StatusCodes.NotFound, "Employer not found")
        case Success(EmployerLookupUnavailable) | Failure(_: ServiceUnavailable) | Failure(_: TimeoutException) =>
          complete(StatusCodes.ServiceUnavailable, "Employer lookup temporarily unavailable")
        case Failure(error) => failWith(error)
      }

    get {
      concat(
        path("employer") { employerById(DemoEmployers.DefaultId) },
        path("employer" / Segment) { id => employerById(id) },
        path("employerToScreen") {
          onSuccess(queueDemo()) {
            complete(StatusCodes.Accepted, "Demo request queued")
          }
        }
      )
    }
  }
}
