package microservices

import org.apache.pekko.actor.{Actor, ActorLogging}

/** An actor that registers itself when all of its service dependencies are available.
  *
  * @param id service name used for registration
  * @param dependencies names of the services required by this actor
  */
abstract class Microservice(id: String, dependencies: Set[String]) extends Actor with ActorLogging {
  private var currentStatus = MicroserviceStatus.Stopped

  protected final def isRunning: Boolean = currentStatus == MicroserviceStatus.Running

  override def preStart(): Unit =
    log.info("Microservice {} started with status {}", id, currentStatus)

  override def receive: Receive = microserviceReceive.orElse(userReceive)

  def microserviceReceive: Receive = {
    case RunningMicroservices(serviceNames) => changeStatus(serviceNames)
  }

  protected def onBecameRunning(): Unit = ()

  protected def onBecameStopped(): Unit = ()

  def userReceive: Receive

  def changeStatus(serviceNames: Set[String]): Unit = {
    val dependenciesAvailable = dependencies.subsetOf(serviceNames)

    if (dependenciesAvailable && !isRunning) {
      currentStatus = MicroserviceStatus.Running
      ServiceRegistryExtension(context.system).register(id, self)
      log.info("Microservice {} changed its status to RUNNING", id)
      onBecameRunning()
    } else if (!dependenciesAvailable && isRunning) {
      currentStatus = MicroserviceStatus.Stopped
      ServiceRegistryExtension(context.system).terminate(self)
      log.warning("Microservice {} changed its status to STOPPED because a dependency is unavailable", id)
      onBecameStopped()
    }
  }
}

enum MicroserviceStatus:
  case Running
  case Stopped

/** Service names with at least one registered instance in the current snapshot. */
case class RunningMicroservices(serviceNames: Set[String]) extends CborSerializable
