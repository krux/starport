package com.krux.starport.lambda

import scala.beans.BeanProperty

case class Response(@BeanProperty message: String, @BeanProperty request: Request)
