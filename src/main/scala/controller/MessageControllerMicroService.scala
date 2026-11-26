package controller

import entity.DatabaseMicroService.EmployerChecked
import controller.MessageControllerMicroService.CheckEmployer
import entity.Employer
import microservices.{CborSerializable, Microservice}

class MessageControllerMicroService(id: String, dependencies: Set[String]) extends Microservice(id, dependencies) {

  override def userReceive: Receive = {
    case CheckEmployer(employer) =>
      if (employer.yearsOperating > 18) sender() ! EmployerChecked(employer)
  }
}

object MessageControllerMicroService {

  case class CheckEmployer(employer: Employer) extends CborSerializable

}
