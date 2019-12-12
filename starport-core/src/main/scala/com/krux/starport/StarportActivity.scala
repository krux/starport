package com.krux.starport

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.table.ScheduledPipelines
import com.krux.starport.db.{DateTimeMapped, WaitForIt}
import com.krux.starport.util.DateTimeFunctions


trait StarportActivity extends DateTimeMapped with WaitForIt with Logging with DateTimeFunctions {

  // use -Dconf.resource=application.dev.conf for testing
  implicit val conf = StarportSettings()

  def db = conf.jdbc.db
  val parallel = conf.parallel

  def inConsoleManagedPipelineIds(): Set[String] = {
    val pipelineIds = db.run(
      ScheduledPipelines()
        .filter(_.inConsole)
        .distinctOn(_.awsId)
        .result
    ).waitForResult.map(_.awsId).toSet
    logger.info(s"Retrieved ${pipelineIds.size} in-console managed pipelines from Starport DB.")
    pipelineIds
  }

}
