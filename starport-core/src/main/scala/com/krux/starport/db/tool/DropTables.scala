package com.krux.starport.db.tool

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.table._
import com.krux.starport.db.WaitForIt
import com.krux.starport.Logging

object DropTables extends App with Logging with WaitForIt with Schema {

  logger.info("Drop all tables...")

  args(0).toBoolean match {
    case true =>  // live
      val jdbcConfig = StarportSettings().jdbc
      logger.info("Running the following statements")
      schema.drop.statements.foreach(println)
      jdbcConfig.db.run(DBIO.seq(schema.drop)).waitForResult
    case false =>  // dry run
      schema.drop.statements.foreach(println)
  }

  logger.info("All tables dropped")

}
