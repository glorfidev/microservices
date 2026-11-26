package jobdemo

import entity.DemoEmployers
import microservices.{CborSerializable, ServiceRegistryExtension}
import org.apache.pekko.actor.{Actor, ActorRef, ActorSystem, Cancellable, Props}
import org.apache.pekko.pattern.{ask, pipe}
import org.apache.pekko.persistence.PersistentActor
import org.apache.pekko.util.Timeout

import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.Try

private[jobdemo] object JobWorkflow {
  val ServiceId = "job-workflow"
  val ReviewServiceId = "posting-review"
  val Employers = DemoEmployers.records.map(org => org.publicId -> org.name).toMap

  final case class Submit(id: String, employerId: String, title: String, positions: Int)
  case object ListPostings
  final case class Submission(posting: Posting, created: Boolean)
  final case class InvalidSubmission(reason: String)
  final case class SubmissionConflict(reason: String)
  final case class Posting(
      id: String,
      employerId: String,
      title: String,
      positions: Int,
      submittedAt: Long,
      decision: Option[Decision] = None
  ) {
    def status: String = decision.fold("Pending")(result => if (result.approved) "Approved" else "Rejected")
    def matches(command: Submit): Boolean =
      employerId == command.employerId && title == command.title && positions == command.positions
  }
  final case class Decision(approved: Boolean, reason: String, decidedAt: Long)

  final case class Review(positions: Int) extends CborSerializable
  final case class Recommendation(approved: Boolean, reason: String) extends CborSerializable

  final case class PostingSubmitted(
      id: String, employerId: String, title: String, positions: Int, submittedAt: Long
  ) extends CborSerializable
  final case class DecisionRecorded(
      id: String, approved: Boolean, reason: String, decidedAt: Long
  ) extends CborSerializable

  private case object ReviewTick
  private final case class Reviewed(id: String, result: Try[Recommendation])

  def props(
      findReviewer: ActorSystem => Future[ActorRef] =
        system => ServiceRegistryExtension(system).lookupLocal(ReviewServiceId),
      reviewDelay: FiniteDuration = 5.seconds
  ): Props = Props(new JobWorkflow(findReviewer, reviewDelay))

  val reviewProps: Props = Props(new Actor {
    override def receive: Receive = {
      case Review(positions) =>
        if (positions <= 5) sender() ! Recommendation(true, "Request is within the five-position demo limit.")
        else sender() ! Recommendation(false, "Requests for more than five positions need a separate review.")
    }
  })
}

/** One durable writer for the local demo. Only pending postings are eligible for review. */
private[jobdemo] class JobWorkflow(
    findReviewer: ActorSystem => Future[ActorRef],
    reviewDelay: FiniteDuration
) extends PersistentActor {
  import JobWorkflow.*
  import context.dispatcher

  override val persistenceId: String = "job-postings-v1"
  private implicit val timeout: Timeout = Timeout(2.seconds)
  private var postings = Map.empty[String, Posting]
  private var reviewing = Option.empty[String]
  private var tick = Option.empty[Cancellable]

  override def preStart(): Unit = {
    super.preStart()
    tick = Some(context.system.scheduler.scheduleWithFixedDelay(300.millis, 600.millis, self, ReviewTick))
  }

  override def postStop(): Unit = {
    tick.foreach(_.cancel())
    super.postStop()
  }

  private def submitted(event: PostingSubmitted): Unit =
    postings += event.id -> Posting(event.id, event.employerId, event.title, event.positions, event.submittedAt)

  private def recorded(event: DecisionRecorded): Unit = {
    val posting = postings(event.id)
    postings += event.id -> posting.copy(decision = Some(Decision(event.approved, event.reason, event.decidedAt)))
  }

  override def receiveRecover: Receive = {
    case event: PostingSubmitted => submitted(event)
    case event: DecisionRecorded => recorded(event)
  }

  override def receiveCommand: Receive = {
    case raw: Submit =>
      val command = raw.copy(title = raw.title.trim)
      val replyTo = sender()
      postings.get(command.id) match {
        case Some(existing) if existing.matches(command) => replyTo ! Submission(existing, created = false)
        case Some(_) => replyTo ! SubmissionConflict("This submission ID already belongs to another posting.")
        case None if !command.id.matches("[a-fA-F0-9-]{36}") || Try(java.util.UUID.fromString(command.id)).isFailure =>
          replyTo ! InvalidSubmission("A valid submission ID is required.")
        case None if !Employers.contains(command.employerId) =>
          replyTo ! InvalidSubmission("Choose a fictional demo employer.")
        case None if command.title.isEmpty || command.title.length > 100 =>
          replyTo ! InvalidSubmission("Enter a title between 1 and 100 characters.")
        case None if command.positions < 1 || command.positions > 100 =>
          replyTo ! InvalidSubmission("Positions must be between 1 and 100.")
        case None =>
          persist(PostingSubmitted(command.id, command.employerId, command.title, command.positions, System.currentTimeMillis())) {
            event =>
              submitted(event)
              replyTo ! Submission(postings(event.id), created = true)
          }
      }

    case ListPostings =>
      sender() ! postings.values.toList.sortBy(posting => (-posting.submittedAt, posting.id))

    case ReviewTick =>
      if (reviewing.isEmpty) {
        val oldestDue = System.currentTimeMillis() - reviewDelay.toMillis
        postings.valuesIterator
          .filter(posting => posting.decision.isEmpty && posting.submittedAt <= oldestDue)
          .minByOption(posting => (posting.submittedAt, posting.id))
          .foreach { posting =>
            reviewing = Some(posting.id)
            findReviewer(context.system)
              .flatMap(reviewer => (reviewer ? Review(posting.positions)).mapTo[Recommendation])
              .transform(result => scala.util.Success(Reviewed(posting.id, result)))
              .pipeTo(self)
          }
      }

    case Reviewed(id, result) =>
      reviewing = None
      result.toOption.filter(_ => postings.get(id).exists(_.decision.isEmpty)).foreach { recommendation =>
        persist(DecisionRecorded(id, recommendation.approved, recommendation.reason, System.currentTimeMillis()))(recorded)
      }
  }
}
