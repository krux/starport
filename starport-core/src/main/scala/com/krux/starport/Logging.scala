package com.krux.starport

import org.slf4j.LoggerFactory

trait Logging {
  def logger = LoggerFactory.getLogger(getClass)
}
