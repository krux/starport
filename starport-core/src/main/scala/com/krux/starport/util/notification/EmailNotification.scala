package com.krux.starport.util.notification

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline

object EmailNotification extends Notification {

  override def send(summary: String, message: String, pipeline: Pipeline)(implicit conf: StarportSettings): String = {
    val toEmails = pipeline.owner.map(Seq(_)).getOrElse(conf.toEmails)
    val fromEmail = conf.fromEmail

    SendEmail(
      toEmails,
      fromEmail,
      summary,
      message
    )
  }

}
