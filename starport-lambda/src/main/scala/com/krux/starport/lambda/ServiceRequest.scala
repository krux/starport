package com.krux.starport.lambda

import scala.beans.BeanProperty

class ServiceRequest(@BeanProperty var args: Array[String]) {
  def this() = this(Array[String]())
}
