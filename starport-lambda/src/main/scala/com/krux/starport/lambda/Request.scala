package com.krux.starport.lambda

import scala.beans.BeanProperty

class Request(@BeanProperty var key1: String, @BeanProperty var key2: String, @BeanProperty var key3: String) {
  def this() = this("", "", "")
}
