package com.krux.starport.db.tool

import com.krux.hyperion.expression.Duration
import com.krux.starport.{BuildInfo}
import org.joda.time.DateTime
import scopt.OptionParser
import com.krux.starport.cli.Reads
import com.krux.hyperion.cli.Reads._

object SubmitPipelineOptionParser extends Reads {

  val programName = "submit-pipeline"

  private val ValidEmail = """^([a-zA-Z0-9.!#$%&’'*+/=?^_`{|}~-]+)@([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*)$""".r

  def apply(errorHandler:(String, Option[Int]) => Unit): OptionParser[SubmitPipelineOptions] = new OptionParser[SubmitPipelineOptions](programName) {

    head(programName, s"${BuildInfo.version}")

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
        errorHandler("You cannot enable and disable a pipeline at the same time",Some(4))
      }
      c.copy(enable = Option(false))
    }
      .text("disable a pipeline")
      .optional()

    opt[Unit]('e', "enable").action { (_, c) =>
      if (c.enable.nonEmpty) {
        errorHandler("You cannot enable and disable a pipeline at the same time",Some(4))
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

    opt[Unit]("dry-run").action((_, c) => c.copy(dryRun = true))
      .text("do not submit to database")
      .optional()

  }

  def parse(args: Array[String],errorHandler:(String, Option[Int]) => Unit): Option[SubmitPipelineOptions] = apply(errorHandler).parse(args, SubmitPipelineOptions())
}
