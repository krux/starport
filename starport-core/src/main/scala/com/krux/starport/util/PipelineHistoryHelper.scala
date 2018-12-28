package com.krux.starport.util

import scala.concurrent.ExecutionContext

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.Logging
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.WaitForIt
import com.krux.starport.db.record.{Pipeline, PipelineHistory, ScheduledPipeline}
import com.krux.starport.db.table.{PipelineHistories, Pipelines}
import com.krux.starport.util.HealthStatus.HealthStatus


class PipelineHistoryHelper(implicit conf: StarportSettings) extends WaitForIt with Logging {

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

    val insertAction = DBIO.seq(PipelineHistories() ++= pipelineHistories).transactionally
    db.run(insertAction).waitForResult
  }

  def updatePipelineHistories(scheduledPipelineRecords: Seq[ScheduledPipeline], healthStatus: HealthStatus)
    (implicit ec: ExecutionContext): Unit = {
    logger.info(s"Mark pipelines ${scheduledPipelineRecords.map(_.awsId).mkString(",")} as $healthStatus ...")

    val actions = (
      for {
        pipelines <- Pipelines().filter(_.id.inSet(scheduledPipelineRecords.map(_.pipelineId).toSet)).result
        nextRunTimes = pipelines.map(p => (p.id.get, p.nextRunTime)).toMap
        pipelineHistories = scheduledPipelineRecords.map { p =>
          PipelineHistory(
            p.pipelineId,
            nextRunTimes.getOrElse(p.pipelineId, None),
            healthStatus.toString
          )
        }
        result <- PipelineHistories() ++= pipelineHistories
      } yield result).transactionally

    db.run(actions).waitForResult
  }

}
