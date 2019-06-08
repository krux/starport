package com.krux.starport

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormat => JodaDateTimeFormat}
import slick.jdbc.PostgresProfile.api._

import com.krux.starport.cli.{CleanupNonStarportOptionParser, CleanupNonStarportOptions}
import com.krux.starport.db.table.ScheduledPipelines
import com.krux.starport.util.{AwsDataPipeline, PipelineStatus, PipelineState}

object CleanupNonStarportPipelines extends StarportActivity {
  final val AwsDateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss"
  final val taskName = "CleanupNonStarportPipelines"

  def pipelineIdsTobeDeleted(
      pipelineState: PipelineState.State,
      cutoffDate: DateTime,
      force: Boolean
    ): Set[String] = {

    logger.info(s"Getting list of old ${pipelineState} non-Starport pipelines from AWS to delete...")
    val dateTimeFormatter = JodaDateTimeFormat.forPattern(AwsDateTimeFormat)
    val inConsoleStarportScheduledPipelineIds = db.run(
        ScheduledPipelines()
          .filter(_.inConsole)
          .distinctOn(_.awsId)
          .result
      ).waitForResult.map(_.awsId).toSet

    logger.info(s"Retrieved ${inConsoleStarportScheduledPipelineIds.size} in console pipelines from Starport DB.")

    def shouldPipelineBeDeleted(pipelineStatus: Option[PipelineStatus]): Boolean = {
      val st = for {
        ps <- pipelineStatus
        s <- ps.pipelineState
        t <- ps.creationTime
      } yield (s, t)

      st.exists { case (s, t) =>
        s == pipelineState && dateTimeFormatter.parseDateTime(t) < cutoffDate.withTimeAtStartOfDay
      }
    }

    val pipelinesInAws =
      if (force) AwsDataPipeline.listPipelineIds()
      else AwsDataPipeline.listPipelineIds() -- inConsoleStarportScheduledPipelineIds

    val pipelineStatuses = AwsDataPipeline.describePipeline(pipelinesInAws.toSeq: _*)
    pipelinesInAws.filter { pId => shouldPipelineBeDeleted(pipelineStatuses.get(pId)) }
  }

  def deletePipelines(ids: Set[String], dryRun: Boolean): Unit = {

    val query = ScheduledPipelines().filter(sp => sp.inConsole && sp.awsId.inSet(ids))

    if (dryRun) {
      println(s"Dry run option is enabled. Otherwise, these AWS pipeline IDs would be deleted:\n${ids.mkString("\n")}")
      val updateCount = db.run(query.length.result).waitForResult
      println(s"It will also update the status of $updateCount pipeline statuses in DB")
    } else {
      AwsDataPipeline.deletePipelines(ids)
      val resultCount = db.run(query.map(_.inConsole).update(false)).waitForResult
      println(s"Updated $resultCount in DB")
    }
  }

  def run(options: CleanupNonStarportOptions) = {
    logger.info(s"run with options: $options")

    val ids = pipelineIdsTobeDeleted(options.pipelineState, options.cutoffDate, options.force)
    logger.info(s"${ids.size} pipelines found")

    deletePipelines(ids, options.dryRun)
  }

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    CleanupNonStarportOptionParser.parse(args) match {
      case Some(options) => run(options)
      case None => ErrorExit.invalidCommandlineArguments(logger)
    }
    val timeSpan = (System.nanoTime - start) / 1E9
    logger.info(s"All old and FINISHED non-Starport pipelines cleaned up in $timeSpan seconds")
  }
}
