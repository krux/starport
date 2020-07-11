package com.krux.starport.db.tool

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record._
import com.krux.starport.db.table._
import com.krux.starport.db.WaitForIt
import com.krux.starport.Logging
import java.time.LocalDateTime


object PopulateTables extends App with Logging with WaitForIt with Schema {

  val jdbcConfig = StarportSettings().jdbc

  logger.info("Populating tables with test data...")

  val pipelinesTable = Pipelines()

  val populate = DBIO.seq(
    pipelinesTable += Pipeline(None, "test1", "s3://some-bucket/some-jar.jar", "com.krux.pipeline.SomePipeline1", false, 3, LocalDateTime.of(2015, 1, 1, 0, 0, 0), "1 day", None, Option(LocalDateTime.of(2015, 12, 11, 0, 0, 0)), true, None),
    pipelinesTable += Pipeline(None, "test2", "s3://some-bucket/some-other-jar.jar", "com.krux.pipeline.SomePipeline2", false, 3, LocalDateTime.of(2015, 2, 1, 0, 0, 0), "1 day", None, Option(LocalDateTime.of(2015, 12, 12, 0, 0, 0)), true, None),
    pipelinesTable += Pipeline(None, "test3", "test3.jar", "com.krux.pipeline.SomePipeline3", false, 3, LocalDateTime.of(2015, 3, 1, 0, 0, 0), "1 day", None, Option(LocalDateTime.of(2016, 12, 12, 0, 0, 0)), true, None),
    pipelinesTable += Pipeline(None, "test4", "test4.jar", "com.krux.pipeline.SomePipeline4", false, 3, LocalDateTime.of(2015, 4, 1, 0, 0, 0), "1 day", Some(LocalDateTime.of(2015, 5, 1, 0, 0, 0)), Option(LocalDateTime.of(2015, 12, 12, 0, 0, 0)), true, None),
    pipelinesTable += Pipeline(
      None,
      "com.krux.lab.pipelines.TestSparkPipeline",
      "s3://some-bucket/some-new-jar.jar",
      "com.krux.lab.pipelines.TestSparkPipeline",
      true,
      3,
      LocalDateTime.of(2015, 4, 1, 0, 0, 0),
      "1 month",
      None,
      Some(LocalDateTime.of(2016, 1, 5, 0, 0, 0)),
      true,
      None
    )
  )

  jdbcConfig.db.run(populate).waitForResult

  logger.info("Testing table populated")

}
