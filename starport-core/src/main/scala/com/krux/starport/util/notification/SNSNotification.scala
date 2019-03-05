package com.krux.starport.util.notification

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline

object SNSNotification extends Notification {
  override def send(summary: String, message: String, pipeline: Pipeline)(implicit conf: StarportSettings): String = {
    val snsTopicARN = pipeline.owner.getOrElse(conf.snsTopicARN)

    SendSNS(
      snsTopicARN,
      s"$summary - $message"
    )
  }
}
