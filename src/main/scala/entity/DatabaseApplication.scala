package entity

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ddata.DistributedData
import com.typesafe.config.ConfigFactory
import microservices.ServiceRegistryExtension

object DatabaseApplication {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseString("pekko.remote.artery.canonical.port=3000").
      withFallback(ConfigFactory.load())

    val actorSystem = ActorSystem("ClusterSystem", config)

    Cluster(actorSystem).registerOnMemberUp {
      DistributedData(actorSystem)

      Database.init()

      val databaseService = actorSystem.actorOf(Props(new DatabaseMicroService("databaseMicroservices", Set("controllerMicroservices"))))

      ServiceRegistryExtension(actorSystem).subscribe(databaseService)
    }
  }


}
