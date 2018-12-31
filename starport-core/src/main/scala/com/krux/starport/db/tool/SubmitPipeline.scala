package com.krux.starport.db.tool

import java.io.File
import java.net.URLClassLoader

import com.github.nscala_time.time.Imports._
import scopt.OptionParser
import slick.jdbc.PostgresProfile.api._

import com.krux.hyperion.cli.Reads._
import com.krux.hyperion.expression.Duration
import com.krux.hyperion.{DataPipelineDefGroup, RecurringSchedule, Schedule}
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline
import com.krux.starport.db.table.Pipelines
import com.krux.starport.db.{DateTimeMapped, WaitForIt}
import com.krux.starport.util.notification.SendSlackMessage
import com.krux.starport.util.{DateTimeFunctions, S3FileHandler}
import com.krux.starport.{BuildInfo, ErrorExit, Logging}


object SubmitPipeline extends DateTimeFunctions with WaitForIt with DateTimeMapped with Logging {

  private val ValidEmail = """^([a-zA-Z0-9.!#$%&’'*+/=?^_`{|}~-]+)@([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*)$""".r

  lazy val starportSettings = StarportSettings()

  case class Options(
    jar: String = "",
    pipelineObject: String = "",
    startNow: Boolean = false,
    update: Boolean = false,
    noBackfill: Boolean = false,
    baseDir: Option[String] = None,
    cleanUp: Boolean = false,
    schedule: Option[DateTime] = None,
    frequency: Option[Duration] = None,
    enable: Option[Boolean] = None,
    owner: Option[String] = None,
    dryRun: Boolean = false
  ) {
    def backfill = !noBackfill
  }

  private def getPipelineSchedule = (jarFile: File, cli: Options) => {
    val jars = Array(jarFile.toURI.toURL)
    val classLoader = new URLClassLoader(jars, this.getClass.getClassLoader)
    val pipelineDef = classLoader.loadClass(cli.pipelineObject + "$")
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

  def main(args: Array[String]): Unit = {

    val parser = new OptionParser[Options]("submit-pipeline") {

      head("submit-pipeline", s"${BuildInfo.version}")
      help("help") text("prints this usage text")

      opt[String]('j', "jar").action((x, c) => c.copy(jar = x))
        .text("the jar to be used")
        .required()

      opt[String]('p', "pipeline").action((x, c) => c.copy(pipelineObject = x))
        .text("the pipeline object")
        .required()

      opt[Unit]('n', "now").action((_, c) => c.copy(startNow = true))
        .text("if the submitted pipeline should start before now")
        .optional()

      opt[Unit]('u', "update").action((_, c) => c.copy(update = true))
        .text("""perform an update to the pipeline's schedule and enables the pipeline
          |        (if it's previously disabled) instead of insert""".stripMargin)
        .optional()

      opt[Unit]("no-backfill").action((_, c) => c.copy(noBackfill = true))
        .text("do not perform any backfill when scheduling")
        .optional()

      opt[String]('b', "dir").action((x, c) => c.copy(baseDir = Option(x)))
        .text("temporary directory to use")
        .optional()

      opt[Unit]('c', "cleanup").action((_, c) => c.copy(cleanUp = true))
        .text("clean up the temporary directory when done")
        .optional()

      opt[DateTime]('s', "schedule").action((x, c) => c.copy(schedule = Option(x)))
        .text("""the time to run the pipeline in the format HH:mm:ss eg -s T23:45:00 will
          |        mean 11:45pm""".stripMargin)
        .optional()

      opt[Duration]('f', "frequency").action((x, c) => c.copy(frequency = Option(x)))
        .text("the frequency of the pipeline in days. eg -f \"2 days\"")
        .optional()

      opt[Unit]('d', "disable").action { (_, c) =>
          if (c.enable.nonEmpty) {
            ErrorExit.invalidPipelineEnableOptions(logger)
          }
          c.copy(enable = Option(false))
        }
        .text("disable a pipeline")
        .optional()

      opt[Unit]('e', "enable").action { (_, c) =>
          if (c.enable.nonEmpty) {
            ErrorExit.invalidPipelineEnableOptions(logger)
          }
          c.copy(enable = Option(true))
        }
        .text("""enable a pipeline, note that this will perform backfill between the
          |        current date time and the last success run time, make sure you
          |        know what you're doing. If you do not want to perform backfill,
          |        use the -u flag instead""".stripMargin)
        .optional()

      opt[String]('o', "owner").action((x, c) => c.copy(owner = Option(x)))
        .text("the owner's email address")
        .validate(_ match {
          case ValidEmail(_, _) => success
          case _ => failure("Option --owner must be a valid email address")
        })
        .optional()

      opt[Unit]("dry-run").action((x, c) => c.copy(dryRun = true))
        .text("do not submit to database")
        .optional()

    }

    parser.parse(args, Options()).foreach { cli =>

      def runQuery[T](query: DBIO[T], dryRunOutput: T, force: Boolean = false): T = {
        if (cli.dryRun && !force) {
          println("Dry Run. Skip sending the querys...")
          dryRunOutput
        } else {
          val db = starportSettings.jdbc.db
          db.run(query).waitForResult
        }
      }

      // load the class from the jar and print the schedule
      // val jars = Array(new File(cli.jar).toURI.toURL)
      val jarFile = S3FileHandler.getFileFromS3(cli.jar, cli.baseDir)
      if (cli.cleanUp) jarFile.deleteOnExit

      val (period, start) = (cli.frequency, cli.schedule) match {
        case (Some(freq), Some(schedule)) =>
          val specifiedSchedule = Schedule
            .cron
            .startDateTime(schedule)
            .every(freq)
          (specifiedSchedule.period, specifiedSchedule.start.get.withZone(DateTimeZone.UTC))
        case x =>
          // if the schedule or frequency are not specified then instantiate the pipeline object and read the schedule variable
          val pipelineSchedule = getPipelineSchedule(jarFile, cli)
          // if 'one' of the parameters(schedule / frequency) is specified, then it will override the pipeline's definition of that param
          x match {
            case (Some(freq), None) =>
              (freq, pipelineSchedule.start.get.withZone(DateTimeZone.UTC))
            case (None, Some(schedule)) =>
              (pipelineSchedule.period, schedule)
            case _ =>
              (pipelineSchedule.period, pipelineSchedule.start.getOrElse(DateTime.now).withZone(DateTimeZone.UTC))
          }
      }

      // determine the next run time
      val next =
        if (cli.startNow) previousRunTime(start, period, DateTime.now)
        else nextRunTime(start, period, DateTime.now)

      val pipelineRecord = Pipeline(
        None,
        cli.pipelineObject,
        cli.jar,
        cli.pipelineObject,
        cli.enable.getOrElse(true),
        2,
        start,
        period.toString,
        None,
        Option(next),
        cli.backfill,
        cli.owner
      )

      println(pipelineRecord)

      // first need to make sure there are no name conflicts (we do this at application level) as
      // there might be use case that we need two pipelines with the same name (a special backfill)
      val existingPipelineQuery = Pipelines()
        .filter(p => p.name === cli.pipelineObject || p.`class` === cli.pipelineObject)

      val existingPipelines = runQuery(existingPipelineQuery.result, Seq.empty, force = true).size

      if (cli.update) {

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

      } else if (cli.enable.nonEmpty) {

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

}
