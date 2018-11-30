package com.krux.starport.db.tool

import com.github.nscala_time.time.Imports.DateTime
import slick.jdbc.PostgresProfile.api._

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record._
import com.krux.starport.db.table._
import com.krux.starport.db.WaitForIt
import com.krux.starport.Logging


object PopulateTables extends App with Logging with WaitForIt with Schema {

  val jdbcConfig = StarportSettings().jdbc

  logger.info("Populating tables with test data...")

  val pipelinesTable = Pipelines()

  val populate = DBIO.seq(
    pipelinesTable += Pipeline(None, "test1", "s3://some-bucket/some-jar.jar", "com.krux.pipeline.SomePipeline1", false, 3, new DateTime("2015-01-01"), "1 day", None, Option(new DateTime("2015-12-11")), true, None),
    pipelinesTable += Pipeline(None, "test2", "s3://some-bucket/some-other-jar.jar", "com.krux.pipeline.SomePipeline2", false, 3, new DateTime("2015-02-01"), "1 day", None, Option(new DateTime("2015-12-12")), true, None),
    pipelinesTable += Pipeline(None, "test3", "test3.jar", "com.krux.pipeline.SomePipeline3", false, 3, new DateTime("2015-03-01"), "1 day", None, Option(new DateTime("2016-12-12")), true, None),
    pipelinesTable += Pipeline(None, "test4", "test4.jar", "com.krux.pipeline.SomePipeline4", false, 3, new DateTime("2015-04-01"), "1 day", Some(new DateTime("2015-05-01")), Option(new DateTime("2015-12-12")), true, None),
    pipelinesTable += Pipeline(
      None,
      "com.krux.lab.pipelines.TestSparkPipeline",
      "s3://some-bucket/some-new-jar.jar",
      "com.krux.lab.pipelines.TestSparkPipeline",
      true,
      3,
      new DateTime("2015-04-01"),
      "1 month",
      None,
      Some(new DateTime("2016-01-05")),
      true,
      None
    )
  )

  jdbcConfig.db.run(populate).waitForResult

  logger.info("Testing table populated")

}
