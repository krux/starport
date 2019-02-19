package com.krux.starport.lambda

import scala.beans.BeanProperty

class SubmitRequest(@BeanProperty var args: Array[String]) {
  def this() = this(Array[String]())
}
