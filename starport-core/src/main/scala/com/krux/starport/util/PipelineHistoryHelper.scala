package com.krux.starport.util

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.Logging
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.WaitForIt
import com.krux.starport.db.record.{Pipeline, PipelineHistory, ScheduledPipeline}
import com.krux.starport.db.table.{PipelineHistories, Pipelines}
import com.krux.starport.util.HealthStatus.HealthStatus


object PipelineHistoryHelper extends WaitForIt with Logging {

  implicit val conf = StarportSettings()

  def db = conf.jdbc.db

  def addPipelineHistories(pipelineRecords: Seq[Pipeline], healthStatus: HealthStatus): Unit = {
    logger.info(s"Mark pipelines ${pipelineRecords.map(_.id).mkString(",")} as $healthStatus ...")

    val pipelineHistories = pipelineRecords.map { p =>
      PipelineHistory(
        p.id.get,
        p.nextRunTime,
        healthStatus.toString
      )
    }

    val insertAction = DBIO.seq(PipelineHistories() ++= pipelineHistories)
    db.run(insertAction).waitForResult
  }

  def updatePipelineHistories(scheduledPipelineRecords: Seq[ScheduledPipeline], healthStatus: HealthStatus): Unit = {
    logger.info(s"Mark pipelines ${scheduledPipelineRecords.map(_.awsId).mkString(",")} as $healthStatus ...")

    val query = Pipelines()
      .filter(_.id.inSet(scheduledPipelineRecords.map(_.pipelineId).toSet))
      .take(scheduledPipelineRecords.length)

    val nextRunTimes = db.run(query.result).waitForResult.map(p => (p.id.get, p.nextRunTime)).toMap

    val pipelineHistories = scheduledPipelineRecords.map { p =>
      PipelineHistory(
        p.pipelineId,
        nextRunTimes.getOrElse(p.pipelineId, None),
        healthStatus.toString
      )
    }

    val insertAction = DBIO.seq(PipelineHistories() ++= pipelineHistories)
    db.run(insertAction).waitForResult
  }

}
