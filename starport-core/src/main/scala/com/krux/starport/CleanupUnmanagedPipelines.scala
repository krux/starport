package com.krux.starport

import java.time.format.DateTimeFormatter
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

import slick.jdbc.PostgresProfile.api._

import com.krux.starport.cli.{CleanupUnmanagedOptionParser, CleanupUnmanagedOptions}
import com.krux.starport.db.table.ScheduledPipelines
import com.krux.starport.util.{AwsDataPipeline, PipelineStatus, PipelineState}


object CleanupUnmanagedPipelines extends StarportActivity {
  final val AwsDateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss"

  def pipelineIdsToDelete(
      excludePrefixes: Seq[String],
      pipelineState: PipelineState.State,
      cutoffDate: LocalDateTime,
      force: Boolean
    ): Set[String] = {

    logger.info(s"Getting list of old ${pipelineState} unmanaged pipelines from AWS to delete...")
    val dateTimeFormatter = DateTimeFormatter.ofPattern(AwsDateTimeFormat)

    def shouldPipelineBeDeleted(pipelineStatus: Option[PipelineStatus]): Boolean = {
      val nst = for {
        ps <- pipelineStatus
        n = ps.name
        s <- ps.pipelineState
        t <- ps.creationTime
      } yield (n, s, t)

      nst.exists { case (n, s, t) =>
        (force && n.startsWith(conf.pipelinePrefix) || !excludePrefixes.exists(n.startsWith)) &&
          s == pipelineState &&
          LocalDateTime.parse(t, dateTimeFormatter).isBefore(cutoffDate.truncatedTo(ChronoUnit.DAYS))
      }
    }

    val pipelinesInAws =
      if (force) AwsDataPipeline.listPipelineIds()
      else AwsDataPipeline.listPipelineIds() -- inConsoleManagedPipelineIds()

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

  def run(options: CleanupUnmanagedOptions) = {
    logger.info(s"run with options: $options")

    val ids = pipelineIdsToDelete(options.excludePrefixes, options.pipelineState, options.cutoffDate, options.force)
    logger.info(s"${ids.size} pipelines found")

    deletePipelines(ids, options.dryRun)
  }

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    CleanupUnmanagedOptionParser.parse(args) match {
      case Some(options) => run(options)
      case None => ErrorExit.invalidCommandlineArguments(logger)
    }
    val timeSpan = (System.nanoTime - start) / 1E9
    logger.info(s"All old and FINISHED unmanaged pipelines cleaned up in $timeSpan seconds")
  }
}
