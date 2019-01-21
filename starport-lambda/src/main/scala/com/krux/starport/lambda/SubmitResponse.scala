package com.krux.starport.lambda

import scala.beans.BeanProperty

case class SubmitResponse(@BeanProperty message: String, @BeanProperty request: SubmitRequest)
