package entity


import entity.DatabaseMicroService._
import controller.MessageControllerMicroService.CheckEmployer
import webserver.WritingOnScreenMicroservice.WriteOnScreenThis
import microservices.{CborSerializable, Microservice, ServiceRegistryExtension}

class DatabaseMicroService(id: String, dependencies: Set[String]) extends Microservice(id, dependencies) {

  import context.dispatcher

  override def userReceive: Receive = {

    case CheckDemoEmployer =>
      Db.find(DemoEmployers.DefaultId).foreach { employer =>
        ServiceRegistryExtension(context.system)
          .lookup("controllerMicroservices")
          .foreach(_ ! CheckEmployer(employer))
      }

    case FindEmployer(publicId) =>
      log.info("Finding employer with ID {}", publicId)
      sender() ! EmployerMessage(Db.find(publicId))

    case EmployerChecked(employer) =>
      ServiceRegistryExtension(context.system)
        .lookup("writingOnScreenMicroservice")
        .foreach(_ ! WriteOnScreenThis(employer))

    case SaveEmployer(employer) =>
      Db.save(employer)
      log.info("Saved employer with ID {}", employer.publicId)

    case DeleteEmployer(publicId) =>
      Db.delete(publicId)

    case UpdateEmployer(publicId, name, yearsOperating) =>
      Db.find(publicId)
        .map(_.copy(name = name, yearsOperating = yearsOperating))
        .foreach(Db.save)
  }

}

object DatabaseMicroService {
  case object CheckDemoEmployer extends CborSerializable
  case class SaveEmployer(employer: Employer) extends CborSerializable
  case class FindEmployer(publicId: String) extends CborSerializable
  case class DeleteEmployer(publicId: String) extends CborSerializable
  case class UpdateEmployer(publicId: String, name: String, yearsOperating: Int) extends CborSerializable
  sealed trait EmployerLookupReply extends CborSerializable
  case class EmployerMessage(employer: Option[Employer]) extends EmployerLookupReply
  case object EmployerLookupUnavailable extends EmployerLookupReply
  case class EmployerChecked(employer: Employer) extends CborSerializable
}

