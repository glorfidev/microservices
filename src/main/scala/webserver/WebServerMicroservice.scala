package webserver

import entity.DatabaseMicroService.{EmployerLookupReply, EmployerLookupUnavailable, EmployerMessage, FindEmployer}
import org.apache.pekko.pattern.{ask, pipe}
import org.apache.pekko.util.Timeout
import webserver.WebServerMicroservice.GET
import microservices.{Microservice, ServiceRegistryExtension}
import microservices.ServiceRegistryExtension.ServiceUnavailable

import java.util.concurrent.TimeoutException
import scala.concurrent.duration._
class WebServerMicroservice(id: String, dependencies: Set[String]) extends Microservice(id, dependencies) {

  implicit val timeout: Timeout = Timeout(5.seconds)
  import context.dispatcher

  override def userReceive: Receive = {
    case GET(id) =>
      log.info("Looking up employer with ID {}", id)
      val replyTo = sender()
      ServiceRegistryExtension(context.system)
        .lookup("databaseMicroservices")
        .flatMap(database => (database ? FindEmployer(id)).mapTo[EmployerMessage])
        .recover[EmployerLookupReply] {
          case _: ServiceUnavailable | _: TimeoutException => EmployerLookupUnavailable
        }
        .pipeTo(replyTo)
  }
}

object WebServerMicroservice {

  case class GET(id: String) extends microservices.CborSerializable


}
