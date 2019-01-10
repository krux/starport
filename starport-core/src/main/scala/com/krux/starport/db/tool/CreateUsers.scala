package com.krux.starport.db.tool

import com.typesafe.config.ConfigValueFactory
import scopt.OptionParser
import slick.jdbc.PostgresProfile.api._
import slick.jdbc.PostgresProfile.backend.DatabaseDef

import com.krux.starport.Logging
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.WaitForIt
import com.krux.starport.db.table._

object CreateUsers extends App with Logging with WaitForIt with Schema {

  lazy val starportSettings = StarportSettings()

  case class Options(
    masterUser: String = "",
    masterPassword: String = "",
  )

  def runQuery[T](query: DBIO[T], db: DatabaseDef): T = {
    db.run(query).waitForResult
  }

  def createUser: DBIO[Int] = {
    val userName = starportSettings.config.getString("krux.starport.jdbc.slick.properties.user")
    val password = starportSettings.config.getString("krux.starport.jdbc.slick.properties.password")
    logger.info(s"create role $userName")
    sqlu"CREATE ROLE #$userName WITH LOGIN PASSWORD '#$password'"
  }

  def grantsForUser: DBIO[Int] = {
    val userName = starportSettings.config.getString("krux.starport.jdbc.slick.properties.user")
    val dbName = starportSettings.config.getString("krux.starport.jdbc.slick.properties.databaseName")
    logger.info(s"granting permissions for $userName")
    sqlu"GRANT ALL PRIVILEGES ON DATABASE #$dbName TO #$userName"
  }

  val parser = new OptionParser[Options]("com.krux.starport.db.tool.CreateUsers") {

    help("help") text("prints this usage text")

    opt[String]('u', "masterUser").action((x, c) => c.copy(masterUser = x))
      .text("the db master user")
      .required()

    opt[String]('p', "masterPassword").action((x, c) => c.copy(masterPassword = x))
      .text("the db master password")
      .required()
  }

  parser.parse(args, Options()).foreach { cli =>
    logger.info("Running user/grant statements...")
    val jdbcConfig = starportSettings.config.getConfig("krux.starport.jdbc")
      .withValue("slick.properties.user", ConfigValueFactory.fromAnyRef(cli.masterUser))
      .withValue("slick.properties.password", ConfigValueFactory.fromAnyRef(cli.masterPassword))
    val db: DatabaseDef = Database.forConfig("slick", jdbcConfig)
    runQuery(createUser, db)
    runQuery(grantsForUser, db)
    logger.info("All user/grant statements executed")
  }

}
