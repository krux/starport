package com.krux.starport.util

import com.github.nscala_time.time.Imports._

import org.scalatest.WordSpec


class DateTimeFunctionsSpec extends WordSpec {

  "previousRunTime" should {

    "generate the correct time" in {
      assert(
        DateTimeFunctions.previousRunTime(
          new DateTime("2016-12-25T00:00:00Z"),
          1.week,
          new DateTime("2016-12-19T16:20:00Z")
        ) === new DateTime("2016-12-18T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.previousRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-19T16:20:00Z")
        ) === new DateTime("2016-12-18T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.previousRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-26T16:20:00Z")
        ) === new DateTime("2016-12-18T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.previousRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-11T00:00:00Z")
        ) === new DateTime("2016-12-11T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.previousRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-06T00:00:00Z")
        ) === new DateTime("2016-12-04T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.previousRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-18T00:00:00Z")
        ) === new DateTime("2016-12-18T00:00:00.000Z")
      )


    }
  }

  "nextRunTime" should {

    "generate the correct time" in {

      assert(
        DateTimeFunctions.nextRunTime(
          new DateTime("2016-12-25T00:00:00Z"),
          1.week,
          new DateTime("2016-12-19T16:20:00Z")
        ) === new DateTime("2016-12-25T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.nextRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-19T16:20:00Z")
        ) === new DateTime("2016-12-25T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.nextRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-26T16:20:00Z")
        ) === new DateTime("2017-01-01T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.nextRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-25T00:00:00Z")
        ) === new DateTime("2016-12-25T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.nextRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2017-01-01T00:00:00Z")
        ) === new DateTime("2017-01-01T00:00:00.000Z")
      )

      assert(
        DateTimeFunctions.nextRunTime(
          new DateTime("2016-12-18T00:00:00Z"),
          1.week,
          new DateTime("2016-12-18T00:00:00Z")
        ) === new DateTime("2016-12-18T00:00:00.000Z")
      )

    }

  }


  "timesTillEnd" should {
    "generate the correct number of times" in {

      assert(
        DateTimeFunctions.timesTillEnd(
          new DateTime("2016-12-18T00:00:00Z"),
          new DateTime("2016-12-19T00:00:00Z"),
          1.day
        ) === 1
      )

      assert(
        DateTimeFunctions.timesTillEnd(
          new DateTime("2016-12-18T00:00:00Z"),
          new DateTime("2016-12-19T00:00:00Z"),
          1.hour
        ) === 24
      )

      assert(
        DateTimeFunctions.timesTillEnd(
          new DateTime("2016-12-18T00:00:00Z"),
          new DateTime("2016-12-19T00:00:00Z"),
          30.minutes
        ) === 48
      )

      assert(
        DateTimeFunctions.timesTillEnd(
          new DateTime("2016-12-18T00:00:00Z"),
          new DateTime("2016-12-18T01:00:00Z"),
          1.minutes
        ) === 60
      )

    }

  }
}
