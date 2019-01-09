package com.krux.starport.util.notification

import scala.collection.JavaConverters._
import com.amazonaws.services.simpleemail.AmazonSimpleEmailServiceClientBuilder
import com.amazonaws.services.simpleemail.model._
import com.krux.starport.config.StarportSettings


object SendEmail {

  lazy val starportSettings = StarportSettings()

  def apply(toEmailAddresses: Seq[String], fromAddress: String, subject: String, body: String): String = {
    val client = AmazonSimpleEmailServiceClientBuilder.defaultClient()
    val destination = new Destination().withToAddresses(toEmailAddresses.asJavaCollection)
    val emailMessage = new Message()
      .withBody(new Body().withText(new Content().withData(body)))
      .withSubject(new Content().withData(subject))

    val emailRequest = new SendEmailRequest()
      .withDestination(destination)
      .withMessage(emailMessage)
      .withSource(fromAddress)

    client.sendEmail(emailRequest).getMessageId()
  }
}
