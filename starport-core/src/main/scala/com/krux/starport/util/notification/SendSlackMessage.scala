package com.krux.starport.util.notification

import java.net.{ HttpURLConnection, URL }

import org.json4s.JsonAST.{ JString, JObject }
import org.json4s.jackson.JsonMethods._

object SendSlackMessage {

  def apply(failOnError: Boolean = false, webhookUrl: String = "", user: Option[String] = None,
    message: Seq[String] = Seq.empty, iconEmoji: Option[String] = None, channel: Option[String] = None): Boolean = try {
    //  Setup the connection
    val connection = new URL(webhookUrl).openConnection().asInstanceOf[HttpURLConnection]
    connection.setDoOutput(true)
    connection.setRequestProperty("Content-Type", "application/json")
    connection.setRequestProperty("Accept", "application/json")

    // Write the message
    val output = connection.getOutputStream
    try {
      val slackMessage = Seq(
        "icon_emoji" -> iconEmoji,
        "channel" -> channel,
        "username" -> user,
        "text" -> Option(message.mkString("\n"))
      ).flatMap {
        case (k, None) => None
        case (k, Some(v)) => Option(k -> JString(v))
      }

      output.write(compact(render(JObject(slackMessage: _*))).getBytes)
    } finally {
      output.close()
    }

    // Check the response code
    connection.getResponseCode == 200 || !failOnError
  } catch {
    case e: RuntimeException =>
      System.err.println(e.toString)
      !failOnError
  }
}
