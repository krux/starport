package com.krux.starport.cli

import java.time.LocalDateTime

import org.scalatest.wordspec.AnyWordSpec


class SchedulerOptionParserSpec extends AnyWordSpec {

  val args = Array(
    "--start",
    "2016-03-01T04:00:01Z"
  )

  "SchedulerOptionParser" should {

    "parse start" in {
      val options = SchedulerOptionParser.parse(args).get
      assert(options.scheduledStart === LocalDateTime.of(2016, 3, 1, 4, 0, 1))
    }
  }

}
