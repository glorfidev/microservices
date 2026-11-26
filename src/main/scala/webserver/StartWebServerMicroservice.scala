package webserver

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import entity.DatabaseMicroService.EmployerLookupReply
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import microservices.{Microservice, ServiceRegistryExtension}
import webserver.WebServerMicroservice.GET
import webserver.WritingOnScreenMicroservice.WriteOnScreen

import scala.concurrent.duration.*
import scala.util.{Failure, Success}

class StartWebServerMicroservice(id: String, dependencies: Set[String]) extends Microservice(id, dependencies) {

  private implicit val timeout: Timeout = Timeout(15.seconds)
  private implicit val actorSystem: ActorSystem = context.system

  import context.dispatcher

  private var bindingStarted = false

  override protected def onBecameRunning(): Unit = {
    if (!bindingStarted) {
      bindingStarted = true
      val registry = ServiceRegistryExtension(actorSystem)
      val route = EmployerRoutes(
        employerId =>
          registry.lookup("webserverMicroservices")
            .flatMap(webserver => (webserver ? GET(employerId)).mapTo[EmployerLookupReply]),
        () => registry.lookup("writingOnScreenMicroservice").map(_ ! WriteOnScreen)
      )

      Http().newServerAt("localhost", 8080).bind(route).onComplete {
        case Success(binding) =>
          log.info("Server online at http://{}", binding.localAddress)
        case Failure(error) =>
          log.error(error, "Could not start the HTTP server")
      }
    }
  }

  override def userReceive: Receive = PartialFunction.empty
}
