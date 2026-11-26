package webserver


import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.cluster.Cluster
import org.apache.pekko.cluster.ddata.DistributedData
import com.typesafe.config.ConfigFactory
import microservices.ServiceRegistryExtension
object WebServer {

  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.parseString("pekko.remote.artery.canonical.port=3001").
      withFallback(ConfigFactory.load())

    implicit val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

    Cluster(actorSystem).registerOnMemberUp {
      DistributedData(actorSystem)

      val webserverMicroservice = actorSystem.actorOf(Props(new WebServerMicroservice("webserverMicroservices", Set("databaseMicroservices"))))

      val writingOnScreenMicroservice = actorSystem.actorOf(Props(new WritingOnScreenMicroservice("writingOnScreenMicroservice", Set("databaseMicroservices"))))

      val startWebServerMicroservice = actorSystem.actorOf(Props(new StartWebServerMicroservice("startWebServerMicroservice", Set("writingOnScreenMicroservice", "webserverMicroservices"))))

      val registry = ServiceRegistryExtension(actorSystem)
      registry.subscribe(webserverMicroservice)
      registry.subscribe(writingOnScreenMicroservice)
      registry.subscribe(startWebServerMicroservice)
    }
  }
}
