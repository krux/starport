package com.krux.starport.config

import com.typesafe.config.Config
import slick.jdbc.PostgresProfile.api.Database
import slick.jdbc.PostgresProfile.backend.DatabaseDef


case class JdbcConfig(config: Config) {

  def db: DatabaseDef = Database.forConfig("slick", config)

}
