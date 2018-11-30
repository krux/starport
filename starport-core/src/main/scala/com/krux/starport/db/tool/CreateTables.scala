package com.krux.starport.db.tool

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.table._
import com.krux.starport.db.WaitForIt
import com.krux.starport.Logging

object CreateTables extends App with Logging with WaitForIt with Schema {

  logger.info("Creating tables...")

  args(0).toBoolean match {
    case true =>  // live
      val jdbcConfig = StarportSettings().jdbc
      logger.info("Running the following statements")
      schema.create.statements.foreach(println)
      jdbcConfig.db.run(DBIO.seq(schema.create)).waitForResult
    case false =>  // dry run
      schema.create.statements.foreach(println)
  }

  logger.info("All tables created")

}
