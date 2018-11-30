package com.krux.starport.cli

import org.joda.time.DateTime

import org.scalatest.WordSpec


class SchedulerOptionParserSpec extends WordSpec {

  val args = Array(
    "--start",
    "2016-03-01T04:00:01Z"
  )

  "SchedulerOptionParser" should {

    "parse start" in {
      val options = SchedulerOptionParser.parse(args).get
      assert(options.scheduledStart === new DateTime("2016-02-29T20:00:01.000-08:00"))
    }
  }

}
