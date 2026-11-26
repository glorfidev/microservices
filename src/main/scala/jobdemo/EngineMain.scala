package jobdemo

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.sun.net.httpserver.{HttpExchange, HttpServer}

import java.net.{InetAddress, InetSocketAddress}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path
import java.security.MessageDigest
import java.util.concurrent.{CountDownLatch, Executors, TimeoutException}
import scala.util.control.NonFatal

/** Private protocol between the local Play toolkit and the Pekko engine. */
object EngineMain {
  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Supply the toolkit data directory.")
    val key = sys.env.getOrElse("TOOLKIT_ENGINE_KEY", sys.error("Start the engine through the Play toolkit."))
    val cluster = new DemoCluster(Path.of(args(0)))
    val listener = HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0)
    val executor = Executors.newFixedThreadPool(4)

    sys.addShutdownHook {
      listener.stop(0)
      executor.shutdownNow()
      cluster.close()
    }

    listener.setExecutor(executor)
    val handler = new EngineHttp(cluster, key)
    listener.createContext("/api/", request => handler.handle(request))
    cluster.boot()
    listener.start()
    println(s"TOOLKIT_ENGINE_PORT=${listener.getAddress.getPort}")
    new CountDownLatch(1).await()
  }
}

private[jobdemo] final class EngineHttp(cluster: DemoCluster, key: String) {
  import JobWorkflow.*

  private val json = new ObjectMapper

  def handle(request: HttpExchange): Unit = {
    try {
      val supplied = Option(request.getRequestHeaders.getFirst("X-Engine-Key")).getOrElse("")
      if (!MessageDigest.isEqual(supplied.getBytes(UTF_8), key.getBytes(UTF_8))) {
        respond(request, 403, error("Forbidden."))
      } else {
        val path = request.getRequestURI.getPath.stripPrefix("/api/").split("/").toList
        (request.getRequestMethod, path) match {
          case ("GET", "snapshot" :: Nil) =>
            respond(request, 200, snapshot())
          case ("GET", "jobs" :: Nil) =>
            val jobs = json.createArrayNode()
            cluster.postings().foreach(posting => jobs.add(postingJson(posting)))
            respond(request, 200, json.createObjectNode().set[ObjectNode]("jobs", jobs))
          case ("POST", "jobs" :: Nil) =>
            submit(request)
          case ("POST", "nodes" :: id :: action :: Nil) if action == "start" || action == "stop" =>
            if (cluster.changeNode(id, action == "start")) respond(request, 200, snapshot())
            else respond(request, 404, error("Unknown demo node."))
          case ("POST", "services" :: id :: action :: Nil) if action == "start" || action == "stop" =>
            if (cluster.changeService(id, action == "start")) respond(request, 200, snapshot())
            else respond(request, 404, error("Unknown demo service."))
          case _ =>
            respond(request, 404, error("Unknown toolkit endpoint."))
        }
      }
    } catch {
      case _: WorkflowUnavailable | _: TimeoutException =>
        respond(request, 503, error("The requested service is temporarily unavailable."))
      case _: JsonProcessingException =>
        respond(request, 400, error("Send a valid JSON object."))
      case NonFatal(error) =>
        System.err.println(s"Toolkit operation failed: ${error.getClass.getSimpleName}")
        respond(request, 500, this.error("The operation could not be completed."))
    } finally request.close()
  }

  private def submit(request: HttpExchange): Unit = {
    val bytes = request.getRequestBody.readNBytes(8193)
    if (bytes.length > 8192) {
      respond(request, 413, error("The submission is too large."))
      return
    }

    val input = json.readTree(bytes)
    if (input == null || !input.isObject || !input.path("id").isTextual || !input.path("employerId").isTextual ||
        !input.path("title").isTextual || !input.path("positions").isIntegralNumber ||
        !input.path("positions").canConvertToInt) {
      respond(request, 400, error("Supply a submission ID, employer, title and whole-number position count."))
      return
    }

    cluster.submit(Submit(input.path("id").asText(), input.path("employerId").asText(),
      input.path("title").asText(), input.path("positions").asInt())) match {
      case Submission(posting, created) => respond(request, if (created) 201 else 200, postingJson(posting))
      case InvalidSubmission(reason) => respond(request, 400, error(reason))
      case SubmissionConflict(reason) => respond(request, 409, error(reason))
      case _ => respond(request, 500, error("Unexpected workflow result."))
    }
  }

  private def snapshot(): ObjectNode = {
    val response = json.createObjectNode()
    val nodes = response.putArray("nodes")
    val services = response.putArray("services")
    cluster.snapshot().foreach { node =>
      val view = nodes.addObject().put("id", node.id).put("name", node.name).put("status", node.status)
      node.port.foreach(port => view.put("port", port))
      node.service.foreach { service =>
        services.addObject().put("id", service.id).put("nodeId", node.id)
          .put("status", service.status).put("enabled", service.enabled)
      }
    }
    val employers = response.putArray("employers")
    Employers.toList.sortBy(_._1).foreach { case (id, name) =>
      employers.addObject().put("id", id).put("name", name)
    }
    response
  }

  private def postingJson(posting: Posting): ObjectNode = {
    val result = json.createObjectNode()
      .put("id", posting.id).put("employer", Employers(posting.employerId))
      .put("title", posting.title).put("positions", posting.positions)
      .put("status", posting.status).put("submittedAt", posting.submittedAt)
    posting.decision.foreach(decision => result.put("reason", decision.reason).put("decidedAt", decision.decidedAt))
    result
  }

  private def error(message: String): ObjectNode = json.createObjectNode().put("error", message)

  private def respond(request: HttpExchange, status: Int, body: JsonNode): Unit = {
    val bytes = json.writeValueAsBytes(body)
    request.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
    request.getResponseHeaders.set("Cache-Control", "no-store")
    request.sendResponseHeaders(status, bytes.length.toLong)
    request.getResponseBody.write(bytes)
  }
}
