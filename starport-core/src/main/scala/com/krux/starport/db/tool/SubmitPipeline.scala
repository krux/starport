package com.krux.starport.db.tool

import java.io.File
import java.net.URLClassLoader
import java.security.Permission

import com.github.nscala_time.time.Imports._
import slick.jdbc.PostgresProfile.api._
import com.krux.hyperion.{DataPipelineDefGroup, RecurringSchedule, Schedule}
import com.krux.starport.{ErrorExit, Logging}
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline
import com.krux.starport.db.table.Pipelines
import com.krux.starport.db.{DateTimeMapped, WaitForIt}
import com.krux.starport.util.{DateTimeFunctions, S3FileHandler}
import com.krux.starport.util.notification.SendSlackMessage

sealed case class ExitException(status: Int) extends Exception("[substitute for sys.exit]") {}

/**
  * Override SecurityManager when executing via AWS Lambda, in order to "catch" sys.exit.
  * Note that in the SubmitPipelineOptionParser * when the --help flag is called execution
  * flow will still allow the processing of other flags.
  *
  * If -Dexecution.context=lambda property must is set, main will enable this class
  */
sealed class NoExitSecurityManager extends SecurityManager {
  override def checkPermission(perm: Permission): Unit = {}
  override def checkPermission(perm: Permission, context: Object): Unit = {}
  override def checkExit(status: Int): Unit = {
    throw ExitException(status)
  }
}

object SubmitPipeline extends DateTimeFunctions with WaitForIt with DateTimeMapped with Logging {

  lazy val starportSettings = StarportSettings()

  def main(args: Array[String]): Unit = {
    val executionContext = System.getProperty("execution.context")
    logger.debug(s"args: ${args.mkString(",")}")
    logger.debug(s"context: ${executionContext}")
    if (executionContext == "lambda") {
      System.setSecurityManager(new NoExitSecurityManager)
    }
    SubmitPipelineOptionParser.parse(args, errorHandler) match {
      case Some(options) => {
        run(options)
      }
      case None =>
    }
  }

  def errorHandler(msg: String, code: Option[Int] = Some(1)): Unit = {
    code match {
      case Some(0) =>
      case Some(4) => logger.error(msg)
      case _ => logger.error(s"error occurred: ${msg}")
    }
    System.exit(code.getOrElse(1))
  }

  private def getPipelineSchedule = (jarFile: File, opts: SubmitPipelineOptions) => {
    val jars = Array(jarFile.toURI.toURL)
    val classLoader = new URLClassLoader(jars, this.getClass.getClassLoader)
    val pipelineDef = classLoader.loadClass(opts.pipelineObject + "$")
    // the getField("MODULE$").get(null) is a trick to dynamically load scala objects
    pipelineDef.getField("MODULE$").get(null).asInstanceOf[DataPipelineDefGroup].schedule.asInstanceOf[RecurringSchedule]
  }

  private def sendSlackNotification(message: String) = starportSettings.slackWebhookURL match {
    case Some(webhook) =>
      logger.info("Sending Slack Notification")
      SendSlackMessage(
        webhookUrl = webhook,
        message = Seq(
          "Pipeline " + message,
          ":robot_face: StarportScheduler",
          "Requested By: " + System.getProperties().get("user.name").toString()
        ),
        user = Option("starport"),
        channel = Option("#robots")
      )
    case None =>
      logger.warn("krux.starport.slack_webhook_url not configured, skip sending slack notification")
  }

  def run(opts: SubmitPipelineOptions): Unit = {

    def runQuery[T](query: DBIO[T], dryRunOutput: T, force: Boolean = false): T = {
      if (opts.dryRun && !force) {
        println("Dry Run. Skip sending the querys...")
        dryRunOutput
      } else {
        val db = starportSettings.jdbc.db
        db.run(query).waitForResult
      }
    }

    // Because we are using schedule from CLI it will always be Left of the HDateTime
    // This is a hacky way of solving the incompatibility of hyperion 5.3.0 that changes
    // scheduling start time from joda DateTime to HDateTime
    def getStartTimeFromSchedule(schedule: RecurringSchedule): DateTime = {
      require(schedule.start.isDefined && schedule.start.get.value.isLeft, "Starport does not work with empty or expression based start time")
      schedule.start.get.value.left.get.withZone(DateTimeZone.UTC)
    }

    def getPeriodFromSchedule(schedule: RecurringSchedule): Period = {
      require(schedule.period.value.isLeft, "Starport does not work with expression based period")
      schedule.period.value.left.get
    }


    // load the class from the jar and print the schedule
    // val jars = Array(new File(opts.jar).toURI.toURL)
    val jarFile = S3FileHandler.getFileFromS3(opts.jar, opts.baseDir)
    if (opts.cleanUp) jarFile.deleteOnExit

    val (period, start): (Period, DateTime) = (opts.frequency, opts.schedule) match {
      case (Some(freq), Some(schedule)) =>
        val specifiedSchedule = Schedule
          .cron
          .startDateTime(schedule)
          .every(freq)

        (getPeriodFromSchedule(specifiedSchedule), getStartTimeFromSchedule(specifiedSchedule))
      case x =>
        // if the schedule or frequency are not specified then instantiate the pipeline object and read the schedule variable
        val pipelineSchedule = getPipelineSchedule(jarFile, opts)
        // if 'one' of the parameters(schedule / frequency) is specified, then it will override the pipeline's definition of that param
        x match {
          case (Some(freq), None) =>
            (freq, getStartTimeFromSchedule(pipelineSchedule))
          case (None, Some(schedule)) =>
            (getPeriodFromSchedule(pipelineSchedule), schedule)
          case _ =>
            (getPeriodFromSchedule(pipelineSchedule), getStartTimeFromSchedule(pipelineSchedule))
        }
    }

    // determine the next run time
    val next =
      if (opts.startNow) previousRunTime(start, period, DateTime.now)
      else nextRunTime(start, period, DateTime.now)

    val pipelineRecord = Pipeline(
      None,
      opts.pipelineObject,
      opts.jar,
      opts.pipelineObject,
      opts.enable.getOrElse(true),
      2,
      start,
      period.toString,
      None,
      Option(next),
      opts.backfill,
      opts.owner
    )

    println(pipelineRecord)

    // first need to make sure there are no name conflicts (we do this at application level) as
    // there might be use case that we need two pipelines with the same name (a special backfill)
    val existingPipelineQuery = Pipelines()
      .filter(p => p.name === opts.pipelineObject || p.`class` === opts.pipelineObject)

    val existingPipelines = runQuery(existingPipelineQuery.result, Seq.empty, force = true).size

    if (opts.update) {

      require(pipelineRecord.owner.nonEmpty, "Owner required for updates")

      if (existingPipelines == 0) ErrorExit.pipelineDoesNotExist(logger)
      runQuery(
        existingPipelineQuery
          .map(r => (r.name, r.jar, r.isActive, r.retention, r.period, r.end, r.nextRunTime, r.owner, r.backfill))
          .update((
            pipelineRecord.name,
            pipelineRecord.jar,
            pipelineRecord.isActive,
            pipelineRecord.retention,
            pipelineRecord.period,
            pipelineRecord.end,
            pipelineRecord.nextRunTime,
            pipelineRecord.owner,
            pipelineRecord.backfill
          )),
        0
      )

      sendSlackNotification(
        pipelineRecord.name + " Schedule Updated. Next Run - " + pipelineRecord.nextRunTime
      )

    } else if (opts.enable.nonEmpty) {

      if (existingPipelines == 0) ErrorExit.pipelineDoesNotExist(logger)
      runQuery(
        existingPipelineQuery
          .map(r => r.isActive)
          .update(
            pipelineRecord.isActive
          ),
        0
      )

      sendSlackNotification(
        pipelineRecord.name + (if (pipelineRecord.isActive) " Enabled." else " Disabled.")
      )
    } else {

      if (existingPipelines > 0) ErrorExit.pipelineAlreadyExists(logger)

      require(pipelineRecord.owner.nonEmpty, "Owner required for new pipelines")

      runQuery(DBIO.seq(Pipelines() += pipelineRecord), {})

      sendSlackNotification(
        pipelineRecord.name + " Scheduled. Next Run - " + pipelineRecord.nextRunTime
      )
    }

    logger.info("Done")

  }

}
