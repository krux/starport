package com.krux.starport.lambda

import scala.beans.BeanProperty

case class ServiceResponse(@BeanProperty message: String, @BeanProperty request: ServiceRequest)
