package com.krux.starport.lambda

import scala.beans.BeanProperty

case class SubmitResponse(@BeanProperty stdOut: String, @BeanProperty stdErr: String, @BeanProperty status: Int, @BeanProperty request: SubmitRequest)
