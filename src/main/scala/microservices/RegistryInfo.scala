package microservices

import org.apache.pekko.actor.{Actor, ActorRef, Terminated}

private[microservices] object RegistryInfo {
  case class SubscribeService(actor: ActorRef)
  case class Snapshot(serviceNames: Set[String])
}

/** Delivers a current snapshot on subscription and broadcasts later changes. */
private[microservices] class RegistryInfo extends Actor {
  import RegistryInfo.*

  private var subscribers = Set.empty[ActorRef]
  private var available = Set.empty[String]

  override def receive: Receive = {
    case SubscribeService(actor) =>
      if (!subscribers.contains(actor)) {
        context.watch(actor)
        subscribers += actor
      }
      actor ! RunningMicroservices(available)

    case Snapshot(serviceNames) =>
      if (available != serviceNames) {
        available = serviceNames
        val update = RunningMicroservices(available)
        subscribers.foreach(_ ! update)
      }

    case Terminated(actor) =>
      subscribers -= actor
  }
}
