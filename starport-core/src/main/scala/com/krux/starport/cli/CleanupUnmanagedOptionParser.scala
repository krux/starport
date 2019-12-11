package com.krux.starport.cli

import org.joda.time.DateTime
import scopt.OptionParser

import com.krux.starport.util.PipelineState

object CleanupUnmanagedOptionParser extends Reads {
  val programName = "cleanup-unmanaged-pipelines"

  def apply(): OptionParser[CleanupUnmanagedOptions] = new OptionParser[CleanupUnmanagedOptions](programName) {

    head(programName)
    help("help").text("prints this usage text")

    opt[String]("excludePrefix").valueName("<excludePrefix>")
      .action((x, c) => c.copy(excludePrefixes = c.excludePrefixes :+ x))
      .unbounded()
      .validate(x =>
        if (x.trim.nonEmpty) success
        else failure("Value <excludePrefix> must not be empty")
      )

    opt[PipelineState.State]("pipelineState").valueName("<pipelineState>")
      .action((x, c) => c.copy(pipelineState = x))

    opt[DateTime]("cutoffDate").valueName("<cutoffDate> default value is 2 months before")
      .action((x, c) => c.copy(cutoffDate = x))

    opt[Unit]("dryRun").valueName("<dryRun>")
      .action((_, c) => c.copy(dryRun = true))

    opt[Unit]("force").valueName("<force> will ignore if the pipeline is managed by starport or not")
      .action((_, c) => c.copy(force = true))
  }

  def parse(args: Array[String]): Option[CleanupUnmanagedOptions] = apply().parse(args, CleanupUnmanagedOptions())

}
