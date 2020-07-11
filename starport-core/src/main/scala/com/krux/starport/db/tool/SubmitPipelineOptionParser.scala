package com.krux.starport.db.tool

import java.time.LocalDateTime

import scopt.OptionParser

import com.krux.hyperion.cli.Reads._
import com.krux.hyperion.expression.Duration
import com.krux.starport.cli.Reads
import com.krux.starport.util.notification.Notify
import com.krux.starport.{BuildInfo, ErrorExit, Logging}

object SubmitPipelineOptionParser extends Reads with Logging {

  val programName = "submit-pipeline"

  def apply(): OptionParser[SubmitPipelineOptions] = new OptionParser[SubmitPipelineOptions](programName) {

    head(programName, s"${BuildInfo.version}")

    help("help")

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

    opt[Unit]("update-jar-only").action((_, c) => c.copy(updateJarOnly = true))
      .text("when updating (-u) only update the jar name/location. Use to semantically version jars during deployment")
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

    opt[LocalDateTime]('s', "schedule").action((x, c) => c.copy(schedule = Option(x)))
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
      .text("the owner's email address or SNS topic ARN")
      .validate(Notify.isNameValid(_) match {
        case true => success
        case false => failure("Option --owner must be a valid email address or SNS topic ARN")
      })
      .optional()

    opt[Unit]("dry-run").action((_, c) => c.copy(dryRun = true))
      .text("do not submit to database")
      .optional()

  }

  def parse(args: Array[String]): Option[SubmitPipelineOptions] = apply().parse(args, SubmitPipelineOptions())
}
