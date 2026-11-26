package microservices

import org.apache.pekko.actor._
import org.apache.pekko.cluster.ddata.Replicator._
import org.apache.pekko.cluster.ddata.{DistributedData, ORMultiMap, SelfUniqueAddress}
import org.apache.pekko.event.slf4j.Logger
import org.apache.pekko.pattern.ask
import org.apache.pekko.util.Timeout
import microservices.ServiceRegistry.{Register, ServicesKey, SubscribeMicroservice, Terminate}
import microservices.ServiceRegistryExtension.ServiceUnavailable

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters.*

/** Pekko extension providing access to [[ServiceRegistry]].
  *
  * One instance is created for each actor system. It exposes registration, lookup,
  * unregistration, and subscription operations.
  * @param system owning actor system.
  */
class ServiceRegistryExtension(system: ExtendedActorSystem) extends Extension {

  import system.dispatcher

  private val logger = Logger("ServiceRegistryExtension")
  private val lookupConfig = system.settings.config.getConfig("microservices.registry.lookup")
  private val readTimeout = lookupConfig.getDuration("read-timeout").toScala
  private val replyGrace = lookupConfig.getDuration("reply-grace").toScala

  require(readTimeout > Duration.Zero, "microservices.registry.lookup.read-timeout must be positive")
  require(replyGrace > Duration.Zero, "microservices.registry.lookup.reply-grace must be positive")

  private implicit val lookupTimeout: Timeout = Timeout(readTimeout + replyGrace)

  /** Replicator used to query the distributed service registry. */
  val replicator: ActorRef = DistributedData(system).replicator

  /** Address used by distributed-data updates on this cluster node. */
  implicit val node: SelfUniqueAddress = DistributedData(system).selfUniqueAddress

  /** Actor responsible for updating the distributed registry. */
  val serviceRegistry: ActorRef = system.actorOf(ServiceRegistry.props)

  /** Registers a microservice in distributed data with a specific ID.
    *
    * @param id registry key.
    * @param service actor implementing the service.
    */
  def register(id: String, service: ActorRef): Unit = {
    serviceRegistry ! Register(id, service)
  }

  /** Finds a reference to actor [[Microservice]] by ID.
    *
    * The returned future fails with [[ServiceUnavailable]] when no actor can be found for the ID.
    * @param id service ID.
    * @return future containing a registered actor.
    */
  def lookup(id: String): Future[ActorRef] =
    lookup(id, ReadAll(timeout = readTimeout))

  /** Reads the local replica for callers that can retry a missing or stale binding. */
  def lookupLocal(id: String): Future[ActorRef] =
    lookup(id, ReadLocal)

  private def lookup(id: String, consistency: ReadConsistency): Future[ActorRef] = {
    ask(replicator, Get(ServicesKey, consistency))
      .mapTo[GetResponse[ORMultiMap[String, ActorRef]]]
      .map {
        case response: GetSuccess[ORMultiMap[String, ActorRef]] =>
          response.get(ServicesKey).get(id).flatMap(_.headOption).getOrElse(throw new ServiceUnavailable(id))
        case _: NotFound[?] =>
          throw new ServiceUnavailable(id)
        case _: GetFailure[?] =>
          logger.warn("Distributed registry read did not reach all replicas for {}", id)
          throw new ServiceUnavailable(id)
        case _: GetDataDeleted[?] =>
          logger.warn("Distributed registry data was deleted during lookup of {}", id)
          throw new ServiceUnavailable(id)
      }
  }

  /** Terminates a concrete microservice on demand.
    *
    * @param microservice is reference to Actor to terminate.
    */
  def terminate(microservice: ActorRef): Unit = {
    logger.warn("Unregistering microservice {}", microservice)
    serviceRegistry ! Terminate(microservice)
  }

  def subscribe(actor: ActorRef): Unit = {
    serviceRegistry ! SubscribeMicroservice(actor)
  }
}

object ServiceRegistryExtension
  extends ExtensionId[ServiceRegistryExtension]
  with ExtensionIdProvider {

  final class ServiceUnavailable(serviceId: String)
    extends NoSuchElementException(s"Microservice '$serviceId' is unavailable")

  override def lookup: ExtensionId[? <: Extension] = ServiceRegistryExtension

  override def createExtension(system: ExtendedActorSystem) = new ServiceRegistryExtension(system)

  override def get(system: ActorSystem): ServiceRegistryExtension = super.get(system)
}
