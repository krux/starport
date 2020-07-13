package com.krux.starport.exception

class StarportException(message: String) extends Exception(message) {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

}

