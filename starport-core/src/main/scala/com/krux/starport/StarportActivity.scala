package com.krux.starport

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.{DateTimeMapped, WaitForIt}
import com.krux.starport.util.DateTimeFunctions


trait StarportActivity extends DateTimeMapped with WaitForIt with Logging with DateTimeFunctions {

  // use -Dconf.resource=application.dev.conf for testing
  implicit val conf = StarportSettings()

  def db = conf.jdbc.db
  val parallel = conf.parallel

}
