package microservices

import org.apache.pekko.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import org.apache.pekko.cluster.{Cluster, ClusterEvent}
import org.apache.pekko.cluster.ClusterEvent.LeaderChanged
import org.apache.pekko.cluster.ddata.{DistributedData, ORMultiMap, ORMultiMapKey}
import org.apache.pekko.cluster.ddata.Replicator.*

import scala.concurrent.duration.*

object ServiceRegistry {
  val props: Props = Props[ServiceRegistry]()

  case class Register(name: String, service: ActorRef) extends CborSerializable
  case class Terminate(service: ActorRef) extends CborSerializable
  case class SubscribeMicroservice(actor: ActorRef) extends CborSerializable

  private[microservices] val ServicesKey = ORMultiMapKey[String, ActorRef]("service-bindings")
  private val EmptyServices = ORMultiMap.emptyWithValueDeltas[String, ActorRef]
}

/** Maintains the replicated service bindings and watches registered actors on the cluster leader. */
class ServiceRegistry extends Actor with ActorLogging {
  import ServiceRegistry.*
  import RegistryInfo.{Snapshot, SubscribeService}

  private val distributedData = DistributedData(context.system)
  private val replicator = distributedData.replicator
  private val node = distributedData.selfUniqueAddress
  private val cluster = Cluster(context.system)
  private val registryInfo = context.actorOf(Props[RegistryInfo](), "subscriptions")
  private val writeConsistency = WriteAll(5.seconds)

  private var entries = Map.empty[String, Set[ActorRef]]
  private var watched = Set.empty[ActorRef]
  private var isLeader = false

  override def preStart(): Unit = {
    replicator ! Subscribe(ServicesKey, self)
    cluster.subscribe(self, ClusterEvent.InitialStateAsEvents, classOf[LeaderChanged])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
  }

  override def receive: Receive = {
    case Register(name, service) =>
      replicator ! Update(ServicesKey, EmptyServices, writeConsistency)(_.addBinding(node, name, service))

    case Terminate(service) =>
      unregister(service)

    case Terminated(service) =>
      if (isLeader) unregister(service)

    case changed @ Changed(ServicesKey) =>
      entries = changed.get(ServicesKey).entries
      registryInfo ! Snapshot(entries.keySet)
      refreshWatches()

    case SubscribeMicroservice(actor) =>
      registryInfo ! SubscribeService(actor)

    case LeaderChanged(leader) =>
      isLeader = leader.contains(cluster.selfAddress)
      refreshWatches()

    case _: UpdateSuccess[?] =>
      ()

    case failure: UpdateFailure[?] =>
      log.warning("Registry update could not be confirmed by all replicas: {}", failure)
  }

  private def unregister(service: ActorRef): Unit = {
    // The replicator's state can be newer than our subscription snapshot.
    replicator ! Update(ServicesKey, EmptyServices, writeConsistency) { current =>
      val names = current.entries.iterator.collect { case (name, refs) if refs.contains(service) => name }
      names.foldLeft(current)((map, name) => map.removeBinding(node, name, service))
    }
  }

  private def refreshWatches(): Unit = {
    val desired = if (isLeader) entries.valuesIterator.flatten.toSet else Set.empty[ActorRef]
    (watched -- desired).foreach(context.unwatch)
    (desired -- watched).foreach(context.watch)
    watched = desired
  }
}
