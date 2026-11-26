package microservices

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.{ActorRef, ActorSystem, Props}
import org.apache.pekko.cluster.{Cluster, MemberStatus}
import org.apache.pekko.cluster.ddata.ORMultiMap
import org.apache.pekko.cluster.ddata.Replicator.{Get, GetSuccess, ReadLocal}
import org.apache.pekko.testkit.{TestKit, TestProbe}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.*

class ServiceRegistrySpec extends AnyFunSuite with BeforeAndAfterAll {
  private val config = ConfigFactory.parseString("""
    pekko.remote.artery.canonical.port = 0
    pekko.cluster.seed-nodes = []
    pekko.cluster.jmx.multi-mbeans-in-same-jvm = on
    pekko.cluster.distributed-data.gossip-interval = 200ms
    pekko.cluster.distributed-data.notify-subscribers-interval = 100ms
    pekko.loglevel = INFO
  """).withFallback(ConfigFactory.load())

  private val first = ActorSystem("ServiceRegistrySpec", config)
  private val second = ActorSystem("ServiceRegistrySpec", config)
  private val firstRegistry = ServiceRegistryExtension(first)
  private val secondRegistry = ServiceRegistryExtension(second)
  private val waitForReplication = 8.seconds

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val firstCluster = Cluster(first)
    val secondCluster = Cluster(second)
    firstCluster.join(firstCluster.selfAddress)
    secondCluster.join(firstCluster.selfAddress)
    TestProbe()(using first).awaitAssert({
      assert(firstCluster.state.members.count(_.status == MemberStatus.Up) == 2)
      assert(secondCluster.state.members.count(_.status == MemberStatus.Up) == 2)
    }, 12.seconds)
  }

  override protected def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(second)
    finally {
      try TestKit.shutdownActorSystem(first)
      finally super.afterAll()
    }
  }

  test("replicas share bindings and the service stays available until its last instance stops") {
    val service = "replicated-service"
    val subscription = TestProbe()(using second)
    val observer = TestProbe()(using second)
    val firstInstance = TestProbe()(using first).ref
    val secondInstance = TestProbe()(using second).ref
    val remoteFirstInstance = fromSecondNode(firstInstance)

    secondRegistry.subscribe(subscription.ref)
    assert(!subscription.expectMsgType[RunningMicroservices].serviceNames.contains(service))

    firstRegistry.register(service, firstInstance)
    secondRegistry.register(service, secondInstance)
    subscription.fishForMessage(waitForReplication) {
      case RunningMicroservices(names) => names.contains(service)
      case _ => false
    }
    observer.awaitAssert({
      assert(localInstances(observer, service) == Set(remoteFirstInstance, secondInstance))
    }, waitForReplication)

    first.stop(firstInstance)
    observer.awaitAssert({
      assert(localInstances(observer, service) == Set(secondInstance))
    }, waitForReplication)
    assert(Await.result(secondRegistry.lookup(service), waitForReplication) == secondInstance)
    assert(Await.result(secondRegistry.lookupLocal(service), waitForReplication) == secondInstance)

    second.stop(secondInstance)
    subscription.fishForMessage(waitForReplication) {
      case RunningMicroservices(names) => !names.contains(service)
      case _ => false
    }
    intercept[ServiceRegistryExtension.ServiceUnavailable] {
      Await.result(secondRegistry.lookup(service), waitForReplication)
    }
  }

  test("unregistering before a subscription update removes the newest binding") {
    import ServiceRegistry.{Register, Terminate}

    val writer = TestProbe()(using first)
    val observer = TestProbe()(using second)
    val transient = TestProbe()(using first).ref
    val sentinel = TestProbe()(using first).ref
    val remoteSentinel = fromSecondNode(sentinel)

    writer.send(firstRegistry.serviceRegistry, Register("transient-service", transient))
    writer.send(firstRegistry.serviceRegistry, Terminate(transient))
    writer.send(firstRegistry.serviceRegistry, Register("sentinel-service", sentinel))

    observer.awaitAssert({
      assert(localInstances(observer, "sentinel-service") == Set(remoteSentinel))
      assert(localInstances(observer, "transient-service").isEmpty)
    }, waitForReplication)
    firstRegistry.terminate(sentinel)
  }

  test("a dependent microservice stops and starts again as bindings disappear and return") {
    val dependency = "recovering-dependency"
    val lifecycle = TestProbe()(using second)
    val instance = TestProbe()(using first).ref
    val dependent = second.actorOf(Props(new Microservice("dependent-service", Set(dependency)) {
      override def userReceive: Receive = PartialFunction.empty
      override protected def onBecameRunning(): Unit = lifecycle.ref ! MicroserviceStatus.Running
      override protected def onBecameStopped(): Unit = lifecycle.ref ! MicroserviceStatus.Stopped
    }))
    secondRegistry.subscribe(dependent)

    firstRegistry.register(dependency, instance)
    lifecycle.expectMsg(waitForReplication, MicroserviceStatus.Running)
    firstRegistry.terminate(instance)
    lifecycle.expectMsg(waitForReplication, MicroserviceStatus.Stopped)
    firstRegistry.register(dependency, instance)
    lifecycle.expectMsg(waitForReplication, MicroserviceStatus.Running)
    firstRegistry.terminate(instance)
  }

  private def fromSecondNode(ref: ActorRef): ActorRef = {
    val path = Cluster(first).remotePathOf(ref)
    val remoteRef = second.actorSelection(path).resolveOne(waitForReplication)
    Await.result(remoteRef, waitForReplication + 1.second)
  }

  private def localInstances(probe: TestProbe, name: String): Set[ActorRef] = {
    probe.send(secondRegistry.replicator, Get(ServiceRegistry.ServicesKey, ReadLocal))
    probe.expectMsgType[GetSuccess[ORMultiMap[String, ActorRef]]]
      .get(ServiceRegistry.ServicesKey)
      .get(name)
      .getOrElse(Set.empty)
  }
}
