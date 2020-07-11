package com.krux.starport.cli

import java.time.LocalDateTime

import scopt.OptionParser

object SchedulerOptionParser extends Reads {

  val programName = "start-scheduled-pipelines"

  def apply(): OptionParser[SchedulerOptions] = new OptionParser[SchedulerOptions](programName) {

    head(programName)
    help("help").text("prints this usage text")

    opt[LocalDateTime]("start").valueName("<scheduledStart>")
      .action((x, c) => c.copy(scheduledStart = x))

    opt[LocalDateTime]("end").valueName("<scheduledEnd>")
      .action((x, c) => c.copy(scheduledEnd = x))

    opt[LocalDateTime]("actual-start").valueName("<actualStart>")
      .action((x, c) => c.copy(actualStart = x))

  }

  def parse(args: Array[String]): Option[SchedulerOptions] = apply().parse(args, SchedulerOptions())

}
