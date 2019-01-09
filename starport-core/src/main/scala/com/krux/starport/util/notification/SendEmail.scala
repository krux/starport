package com.krux.starport.util.notification

import scala.collection.JavaConverters._

import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.amazonaws.services.simpleemail.model._


object SendEmail {

  def apply(toEmailAddresses: Seq[String], from: String, subject: String, body: String): String = {
    val client = AmazonSimpleEmailServiceClientBuilder.defaultClient()
    val destination = new Destination().withToAddresses(toEmailAddresses.asJavaCollection)
    val emailMessage = new Message()
      .withBody(new Body().withText(new Content().withData(body)))
      .withSubject(new Content().withData(subject))

    val emailRequest = new SendEmailRequest()
      .withDestination(destination)
      .withMessage(emailMessage)
      .withSource(from)

    client.sendEmail(emailRequest).getMessageId()
  }
}
