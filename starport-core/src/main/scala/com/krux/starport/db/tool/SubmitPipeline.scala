package com.krux.starport.db.tool

//import java.io.File
//import java.net.URLClassLoader

//import com.github.nscala_time.time.Imports._
//import scopt.OptionParser
//import slick.jdbc.PostgresProfile.api._

//import com.krux.hyperion.cli.Reads._
//import com.krux.hyperion.expression.Duration
//import com.krux.hyperion.{DataPipelineDefGroup, RecurringSchedule, Schedule}
//import com.krux.hyperion.{DataPipelineDefGroup, RecurringSchedule}
import com.krux.starport.config.StarportSettings
//import com.krux.starport.db.record.Pipeline
//import com.krux.starport.db.table.Pipelines
import com.krux.starport.db.{DateTimeMapped, WaitForIt}
//import com.krux.starport.util.notification.SendSlackMessage
//import com.krux.starport.util.{DateTimeFunctions, S3FileHandler}
import com.krux.starport.util.{DateTimeFunctions}
//import com.krux.starport.{BuildInfo, ErrorExit, Logging}
import com.krux.starport.{Logging}


object SubmitPipeline extends DateTimeFunctions with WaitForIt with DateTimeMapped with Logging {

  lazy val starportSettings = StarportSettings()

//  private def getPipelineSchedule = (jarFile: File, cli: SubmitPipelineOptions) => {
//    val jars = Array(jarFile.toURI.toURL)
//    val classLoader = new URLClassLoader(jars, this.getClass.getClassLoader)
//    val pipelineDef = classLoader.loadClass(cli.pipelineObject + "$")
//    // the getField("MODULE$").get(null) is a trick to dynamically load scala objects
//    pipelineDef.getField("MODULE$").get(null).asInstanceOf[DataPipelineDefGroup].schedule.asInstanceOf[RecurringSchedule]
//  }
//
//  private def sendSlackNotification(message: String) = starportSettings.slackWebhookURL match {
//    case Some(webhook) =>
//      logger.info("Sending Slack Notification")
//      SendSlackMessage(
//        webhookUrl = webhook,
//        message = Seq(
//          "Pipeline " + message,
//          ":robot_face: StarportScheduler",
//          "Requested By: " + System.getProperties().get("user.name").toString()
//        ),
//        user = Option("starport"),
//        channel = Option("#robots")
//      )
//    case None =>
//      logger.warn("krux.starport.slack_webhook_url not configured, skip sending slack notification")
//  }

  def errorHandler(msg: String, code: Option[Int] = Some(1)): Unit = {
    code match {
      case Some(4) => logger.error(msg)
      case _ => logger.error(s"error occurred: ${msg}")
    }
    System.exit(code.getOrElse(1))
  }

  def main(args: Array[String]): Unit = {
    SubmitPipelineOptionParser.parse(args, errorHandler) match {
      case Some(options) => { run(options) }
      case None => { errorHandler("invalid command line arguments") }
    }

//    parser.parse(args, Options()).foreach { cli =>
//
//      def runQuery[T](query: DBIO[T], dryRunOutput: T, force: Boolean = false): T = {
//        if (cli.dryRun && !force) {
//          println("Dry Run. Skip sending the querys...")
//          dryRunOutput
//        } else {
//          val db = starportSettings.jdbc.db
//          db.run(query).waitForResult
//        }
//      }
//
//      // load the class from the jar and print the schedule
//      // val jars = Array(new File(cli.jar).toURI.toURL)
//      val jarFile = S3FileHandler.getFileFromS3(cli.jar, cli.baseDir)
//      if (cli.cleanUp) jarFile.deleteOnExit
//
//      val (period, start) = (cli.frequency, cli.schedule) match {
//        case (Some(freq), Some(schedule)) =>
//          val specifiedSchedule = Schedule
//            .cron
//            .startDateTime(schedule)
//            .every(freq)
//          (specifiedSchedule.period, specifiedSchedule.start.get.withZone(DateTimeZone.UTC))
//        case x =>
//          // if the schedule or frequency are not specified then instantiate the pipeline object and read the schedule variable
//          val pipelineSchedule = getPipelineSchedule(jarFile, cli)
//          // if 'one' of the parameters(schedule / frequency) is specified, then it will override the pipeline's definition of that param
//          x match {
//            case (Some(freq), None) =>
//              (freq, pipelineSchedule.start.get.withZone(DateTimeZone.UTC))
//            case (None, Some(schedule)) =>
//              (pipelineSchedule.period, schedule)
//            case _ =>
//              (pipelineSchedule.period, pipelineSchedule.start.getOrElse(DateTime.now).withZone(DateTimeZone.UTC))
//          }
//      }
//
//      // determine the next run time
//      val next =
//        if (cli.startNow) previousRunTime(start, period, DateTime.now)
//        else nextRunTime(start, period, DateTime.now)
//
//      val pipelineRecord = Pipeline(
//        None,
//        cli.pipelineObject,
//        cli.jar,
//        cli.pipelineObject,
//        cli.enable.getOrElse(true),
//        2,
//        start,
//        period.toString,
//        None,
//        Option(next),
//        cli.backfill,
//        cli.owner
//      )
//
//      println(pipelineRecord)
//
//      // first need to make sure there are no name conflicts (we do this at application level) as
//      // there might be use case that we need two pipelines with the same name (a special backfill)
//      val existingPipelineQuery = Pipelines()
//        .filter(p => p.name === cli.pipelineObject || p.`class` === cli.pipelineObject)
//
//      val existingPipelines = runQuery(existingPipelineQuery.result, Seq.empty, force = true).size
//
//      if (cli.update) {
//
//        require(pipelineRecord.owner.nonEmpty, "Owner required for updates")
//
//        if (existingPipelines == 0) ErrorExit.pipelineDoesNotExist(logger)
//        runQuery(
//          existingPipelineQuery
//            .map(r => (r.name, r.jar, r.isActive, r.retention, r.period, r.end, r.nextRunTime, r.owner, r.backfill))
//            .update((
//              pipelineRecord.name,
//              pipelineRecord.jar,
//              pipelineRecord.isActive,
//              pipelineRecord.retention,
//              pipelineRecord.period,
//              pipelineRecord.end,
//              pipelineRecord.nextRunTime,
//              pipelineRecord.owner,
//              pipelineRecord.backfill
//            )),
//          0
//        )
//
//        sendSlackNotification(
//          pipelineRecord.name + " Schedule Updated. Next Run - " + pipelineRecord.nextRunTime
//        )
//
//      } else if (cli.enable.nonEmpty) {
//
//        if (existingPipelines == 0) ErrorExit.pipelineDoesNotExist(logger)
//        runQuery(
//          existingPipelineQuery
//            .map(r => r.isActive)
//            .update(
//              pipelineRecord.isActive
//            ),
//          0
//        )
//
//        sendSlackNotification(
//          pipelineRecord.name + (if (pipelineRecord.isActive) " Enabled." else " Disabled.")
//        )
//      } else {
//
//        if (existingPipelines > 0) ErrorExit.pipelineAlreadyExists(logger)
//
//        require(pipelineRecord.owner.nonEmpty, "Owner required for new pipelines")
//
//        runQuery(DBIO.seq(Pipelines() += pipelineRecord), {})
//
//        sendSlackNotification(
//          pipelineRecord.name + " Scheduled. Next Run - " + pipelineRecord.nextRunTime
//        )
//      }
//
//      logger.info("Done")
//
//    }

  }

}
