package toolkit

import play.api.Mode
import play.api.libs.json.{Json, JsValue}
import play.api.mvc.{RequestHeader, Result, Results}
import play.api.routing.sird.*
import play.core.server.{PekkoHttpServer, ServerConfig}

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Path
import java.time.Duration
import java.util.{Locale, UUID}
import java.util.concurrent.{CompletableFuture, CountDownLatch, TimeUnit}
import scala.concurrent.{ExecutionContext, Future}
import scala.io.Source
import scala.jdk.FutureConverters.*
import scala.util.Using
import scala.util.control.NonFatal

object Main {
  def main(args: Array[String]): Unit = {
    require(args.length <= 1, "Supply at most one HTTP port.")
    val port = args.headOption.fold(9090)(_.toInt)
    require(port >= 1024 && port <= 65535, "Use a port between 1024 and 65535.")
    System.setProperty("play.http.secret.key", UUID.randomUUID().toString + UUID.randomUUID().toString)

    val engine = LocalEngine.start()
    val console = new Console(engine, port)
    try {
      val server = PekkoHttpServer.fromRouterWithComponents(
        ServerConfig(port = Some(port), address = "127.0.0.1", mode = Mode.Prod)
      ) { components =>
        import Results.*
        given ExecutionContext = components.actorSystem.dispatcher
        val Action = components.defaultActionBuilder

        val routes: play.api.routing.Router.Routes = {
          case GET(p"/") => Action { request => console.asset(request, "index.html", "text/html; charset=utf-8") }
          case GET(p"/console.css") => Action { request => console.asset(request, "console.css", "text/css; charset=utf-8") }
          case GET(p"/console.js") => Action { request => console.asset(request, "console.js", "text/javascript; charset=utf-8") }
          case GET(p"/api/snapshot") => Action.async { request => console.get(request, "/api/snapshot") }
          case GET(p"/api/jobs") => Action.async { request => console.get(request, "/api/jobs") }
          case POST(p"/api/jobs") => Action.async { request =>
            console.mutate(request) {
              request.body.asJson match {
                case Some(body) => console.forward("POST", "/api/jobs", Some(body))
                case None => Future.successful(BadRequest(Json.obj("error" -> "Send a JSON submission.")))
              }
            }
          }
          case POST(p"/api/nodes/$id/$operation") => Action.async { request =>
            console.control(request, "nodes", id, operation)
          }
          case POST(p"/api/services/$id/$operation") => Action.async { request =>
            console.control(request, "services", id, operation)
          }
        }
        routes
      }

      sys.addShutdownHook {
        try server.stop()
        finally engine.close()
      }
      println(s"Job Cluster Toolkit running at http://127.0.0.1:$port")
      new CountDownLatch(1).await()
    } catch {
      case NonFatal(error) =>
        engine.close()
        throw error
    }
  }
}

private[toolkit] final class Console(engine: LocalEngine, port: Int) {
  import Results.*

  private val client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
  private val access = new LocalAccess(port)

  def asset(request: RequestHeader, file: String, contentType: String): Result = {
    if (!access.allowsHost(request.host)) return Forbidden
    val bytes = Using.resource(getClass.getResourceAsStream(s"/console/$file"))(_.readAllBytes())
    Ok(bytes).as(contentType).withHeaders(
      "Content-Security-Policy" -> "default-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'",
      "X-Content-Type-Options" -> "nosniff",
      "Cache-Control" -> "no-store"
    )
  }

  def get(request: RequestHeader, path: String)(using ExecutionContext): Future[Result] =
    if (access.allowsHost(request.host)) forward("GET", path, None)
    else Future.successful(Forbidden)

  def mutate(request: RequestHeader)(action: => Future[Result]): Future[Result] = {
    if (access.allowsMutation(request.host, request.headers.get("Origin"), request.headers.get("X-Toolkit-Request"))) action
    else Future.successful(Forbidden(Json.obj("error" -> "Use the local toolkit to perform this action.")))
  }

  def control(request: RequestHeader, resource: String, id: String, operation: String)(
      using ExecutionContext
  ): Future[Result] = mutate(request) {
    if (id.matches("[a-z-]{1,40}") && Set("start", "stop").contains(operation)) {
      forward("POST", s"/api/$resource/$id/$operation", None)
    } else Future.successful(NotFound(Json.obj("error" -> "Unknown toolkit operation.")))
  }

  def forward(method: String, path: String, body: Option[JsValue])(using ExecutionContext): Future[Result] = {
    val publisher = body.fold(HttpRequest.BodyPublishers.noBody()) { value =>
      HttpRequest.BodyPublishers.ofString(Json.stringify(value), UTF_8)
    }
    val request = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${engine.port}$path"))
      .timeout(Duration.ofSeconds(24))
      .header("X-Engine-Key", engine.key)
      .header("Content-Type", "application/json")
      .method(method, publisher)
      .build()

    client.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray()).asScala.map { reply =>
      Status(reply.statusCode())(reply.body()).as("application/json; charset=utf-8")
        .withHeaders("Cache-Control" -> "no-store")
    }.recover { case NonFatal(_) =>
      ServiceUnavailable(Json.obj("error" -> "The local demo engine is temporarily unavailable."))
    }
  }
}

private[toolkit] final class LocalAccess(port: Int) {
  private val allowedHosts = Set(s"127.0.0.1:$port", s"localhost:$port")

  def allowsHost(host: String): Boolean = allowedHosts.contains(host.toLowerCase(Locale.ROOT))

  def allowsMutation(host: String, origin: Option[String], intent: Option[String]): Boolean =
    allowsHost(host) && origin.forall(_ == s"http://${host.toLowerCase(Locale.ROOT)}") && intent.contains("1")
}

private[toolkit] final class LocalEngine private (process: Process, val port: Int, val key: String) {
  def close(): Unit = {
    process.destroy()
    if (!process.waitFor(12, TimeUnit.SECONDS)) process.destroyForcibly()
  }
}

private[toolkit] object LocalEngine {
  def start(): LocalEngine = {
    val classpath = sys.props.getOrElse("toolkit.engine.classpath",
      sys.error("Launch with sbt \"toolkit/runMain toolkit.Main\"."))
    val data = sys.props.getOrElse("toolkit.data-dir", ".toolkit-data")
    val binary = if (sys.props("os.name").startsWith("Windows")) "java.exe" else "java"
    val java = Path.of(sys.props("java.home"), "bin", binary)
    val key = UUID.randomUUID().toString
    val launcher = new ProcessBuilder(
      java.toString, "--add-opens=java.base/jdk.internal.misc=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED", "-cp", classpath, "jobdemo.EngineMain", data
    ).redirectErrorStream(true)
    launcher.environment().put("TOOLKIT_ENGINE_KEY", key)
    val process = launcher.start()
    val ready = new CompletableFuture[Int]()
    val output = new Thread(() => {
      Using.resource(Source.fromInputStream(process.getInputStream, "UTF-8")) { stream =>
        stream.getLines().foreach { line =>
          val prefix = "TOOLKIT_ENGINE_PORT="
          if (line.startsWith(prefix)) ready.complete(line.stripPrefix(prefix).toInt)
          else println(s"[demo] $line")
        }
      }
      ready.completeExceptionally(new IllegalStateException("The demo engine exited before it was ready."))
      ()
    }, "toolkit-engine-output")
    output.setDaemon(true)
    output.start()

    try new LocalEngine(process, ready.get(25, TimeUnit.SECONDS), key)
    catch {
      case NonFatal(error) =>
        process.destroy()
        throw error
    }
  }
}
