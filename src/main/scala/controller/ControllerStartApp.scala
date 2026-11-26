package controller

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ddata.DistributedData
import com.typesafe.config.ConfigFactory
import microservices.ServiceRegistryExtension

object ControllerStartApp {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseString("pekko.remote.artery.canonical.port=3002").
      withFallback(ConfigFactory.load())

    implicit val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

    Cluster(actorSystem).registerOnMemberUp {
      DistributedData(actorSystem)

      val controllerMicroservice =
        actorSystem.actorOf(Props(new MessageControllerMicroService("controllerMicroservices", Set.empty)))

      ServiceRegistryExtension(actorSystem).register("controllerMicroservices", controllerMicroservice)
    }
  }
}
