package com.krux.starport.util.notification

import com.amazonaws.services.sns.AmazonSNSClientBuilder
import com.amazonaws.services.sns.model.PublishRequest

object SendSNS {

  def apply(topicArn: String, message: String): String = {
    val client = AmazonSNSClientBuilder.defaultClient()
    val publishRequest = new PublishRequest(topicArn, message)
    client.publish(publishRequest).getMessageId
  }
}
