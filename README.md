# Pekko microservices

A small Scala project for trying out service discovery and recovery on Pekko Cluster.
The Play dashboard lets you stop a node and see what happens to the services and job
postings it was handling.

## Run the dashboard

You'll need JDK 17 or newer and sbt 2.0.3.

```sh
sbt "toolkit/runMain toolkit.Main"
```

Open [http://127.0.0.1:9090](http://127.0.0.1:9090). The toolkit starts a local
three-node cluster. You can start and stop its nodes, pause individual services,
and submit postings from fictional employers. To change the dashboard port, pass
it after `toolkit.Main`. Press Ctrl+C in the terminal to stop the toolkit.

### See a posting survive a restart

1. Wait for all three nodes to come up. In **Services**, pause **Posting review**.
2. Submit a posting. It should stay **Pending**.
3. In **Nodes**, stop and start **Workflow**. The same pending posting should reappear.
4. Resume **Posting review**. A request for up to five positions is approved.
   A larger request is rejected.

Pekko Persistence stores submissions and decisions in a local LevelDB journal under
`.toolkit-data/`. Retrying the same submission ID keeps the original posting.
Play runs in a separate JVM from the Pekko 2 cluster. The dashboard and its API
listen on loopback and manage only the demo cluster.

## Run the employer lookup example

Start the database first, then run the controller and web server in separate terminals.

```sh
sbt "runMain entity.DatabaseApplication"
sbt "runMain controller.ControllerStartApp"
sbt "runMain webserver.WebServer"
```

Once the web server is up, try a lookup for employer `2` and one for a missing ID.

```sh
curl http://localhost:8080/employer/2
# Harbor Demo Analytics

curl -i http://localhost:8080/employer/unknown
# HTTP/1.1 404 Not Found
```

`GET /employer` defaults to employer `1`. Lookups return `503` if a required
service is unavailable or times out. To send the default employer through the
controller, call `curl -i http://localhost:8080/employerToScreen` and watch the
web server log. The employer directory is in memory and resets when the
database restarts.

## How discovery works

Pekko Distributed Data replicates a single map of service names and actor
references. A service stays available while at least one instance is registered.
The cluster leader watches those actors and removes their bindings when they stop.

A registry read waits up to three seconds by default, with another two seconds
for the local replicator to reply. Both durations are configurable in
`application.conf` as `microservices.registry.lookup.read-timeout` and
`microservices.registry.lookup.reply-grace`.

Restart all nodes together when upgrading from Akka or from the older registry
that stored names and actor references separately.

## Tests

```sh
sbt compile 'Test / testFull'
```

The suite includes a two-node registry test, HTTP route tests, and a disk
recovery test that restarts the workflow and checks its decisions.
