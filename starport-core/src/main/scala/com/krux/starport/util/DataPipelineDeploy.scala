package com.krux.starport.util

import java.time.LocalDateTime

import scala.sys.process.{Process, ProcessLogger}

import org.slf4j.Logger

import com.krux.hyperion.client.{AwsClient, AwsClientForName}
import com.krux.hyperion.expression.{Duration => HDuration}
import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.{Pipeline, ScheduledPipeline}
import com.krux.starport.exception.StarportException
import com.krux.starport.util.DateTimeFunctions._

object DataPipelineDeploy {

  def deployAndActive(
    pipeline: Pipeline,
    scheduledStart: LocalDateTime,
    scheduledEnd: LocalDateTime,
    actualStart: LocalDateTime,
    localJar: String,
    starportSetting: StarportSettings,
    logger: Logger
  ): Either[StarportException, Seq[ScheduledPipeline]] = for {
    pipelineName <-
      deployPipeline(pipeline, scheduledStart, scheduledEnd, localJar, starportSetting, logger)
    scheduledPipelines <-
      activatePipeline(pipeline, pipelineName, scheduledStart, actualStart, logger, starportSetting)
  } yield scheduledPipelines

  def deployPipeline(
    pipeline: Pipeline,
    scheduledStart: LocalDateTime,
    scheduledEnd: LocalDateTime,
    localJar: String,
    starportSetting: StarportSettings,
    logger: Logger
  ): Either[StarportException, String] = {
    val start = pipeline.nextRunTime.get
    val until = pipeline.end
      .map(LocalDateTimeOrdering.min(scheduledEnd, _))
      .getOrElse(scheduledEnd)
    val pipelinePeriod = pipeline.period

    // Note that aws datapieline have a weird requirement for endTime (documented as it has to be
    // greater than startTime, but actually it has to be greater than startTime + period), it's
    // very confusing, we here change it to number of times the pipeline should run to avoid this
    // confusion.
    val calculatedTimes =
      if (pipeline.backfill) timesTillEnd(start, until, HDuration(pipelinePeriod))
      else 1

    logger.info(s"calculatedTimes: $calculatedTimes")
    if (calculatedTimes < 1) {
      // the calculatedTimes should never be < 1
      logger.error(s"calculatedTimes < 1")
    }
    val times = Math.max(1, calculatedTimes)

    val actualStart = currentTimeUTC().format(DateTimeFormat)
    val pipelineClass = pipeline.`class`
    val pipelineName =
      s"${starportSetting.pipelinePrefix}${actualStart}_${pipeline.id.getOrElse(0)}_${pipelineClass}"

    // create the pipeline through cli but do not activiate it
    val command = Seq(
      "java",
      "-cp",
      localJar,
      pipelineClass,
      "create",
      "--no-check",
      "--start",
      start.format(DateTimeFormat),
      "--times",
      times.toString,
      "--every",
      pipelinePeriod,
      "--name",
      pipelineName
    ) ++ starportSetting.region.toSeq.flatMap(r => Seq("--region", r.getName))

    val process = Process(
      command,
      None,
      starportSetting.extraEnvs.toSeq: _*
    )

    logger.info(s"Executing `${command.mkString(" ")}`")
    val outputBuilder = new StringBuilder
    val status = process ! ProcessLogger(line => outputBuilder.append(line + "\n"))

    status match {
      case 0 => Right(pipelineName)
      case _ => Left(new StarportException(s"status: $status, logs : ${outputBuilder.toString}"))
    }

  }

  def activatePipeline(
    pipelineRecord: Pipeline,
    pipelineName: String,
    scheduledStart: LocalDateTime,
    actualStart: LocalDateTime,
    logger: Logger,
    conf: StarportSettings
  ): Either[StarportException, Seq[ScheduledPipeline]] = {

    logger.info(s"Activating pipeline: $pipelineName...")

    val awsClientForName = AwsClientForName(AwsClient.getClient(), pipelineName, conf.maxRetry)
    val pipelineIdNameMap = awsClientForName.pipelineIdNames

    awsClientForName
      .forId() match {
      case Some(client) =>
        val activationStatus = client.activatePipelines() match {
          case Some(clientForId) => "success"
          case _ => "fail"
        }

        Right(
          client.pipelineIds.toSeq.map(awsId =>
            ScheduledPipeline(
              awsId,
              pipelineRecord.id.get,
              pipelineIdNameMap(awsId),
              scheduledStart,
              actualStart,
              currentTimeUTC().toLocalDateTime(),
              activationStatus,
              true
            )
          )
        )
      case None =>
        Left(new StarportException(s"pipeline with name $pipelineName not found"))
    }
  }

}
