package com.krux.starport.util

import java.time.{LocalDateTime, Period}

import org.scalatest.wordspec.AnyWordSpec
import java.time.Duration


class DateTimeFunctionsSpec extends AnyWordSpec {

  "previousRunTime" should {

    "generate the correct time" in {
      assert(
        DateTimeFunctions.previousRunTime(
          LocalDateTime.of(2016, 12, 25, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 19, 16, 20, 0)
        ) === LocalDateTime.of(2016, 12, 18, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.previousRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 19, 16, 20, 0)
        ) === LocalDateTime.of(2016, 12, 18, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.previousRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 26, 16, 20, 0)
        ) === LocalDateTime.of(2016, 12, 18, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.previousRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 11, 0, 0, 0)
        ) === LocalDateTime.of(2016, 12, 11, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.previousRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 6, 0, 0, 0)
        ) === LocalDateTime.of(2016, 12, 4, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.previousRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 18, 0, 0, 0)
        ) === LocalDateTime.of(2016, 12, 18, 0, 0, 0)
      )


    }
  }

  "nextRunTime" should {

    "generate the correct time" in {

      assert(
        DateTimeFunctions.nextRunTime(
          LocalDateTime.of(2016, 12, 25, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 19, 16, 20, 0)
        ) === LocalDateTime.of(2016, 12, 25, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.nextRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 19, 16, 20, 0)
        ) === LocalDateTime.of(2016, 12, 25, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.nextRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 26, 16, 20, 0)
        ) === LocalDateTime.of(2017, 1, 1, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.nextRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 25, 0, 0, 0)
        ) === LocalDateTime.of(2016, 12, 25, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.nextRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2017, 1, 1, 0, 0, 0)
        ) === LocalDateTime.of(2017, 1, 1, 0, 0, 0, 0)
      )

      assert(
        DateTimeFunctions.nextRunTime(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          Period.ofWeeks(1),
          LocalDateTime.of(2016, 12, 18, 0, 0, 0)
        ) === LocalDateTime.of(2016, 12, 18, 0, 0, 0)
      )

    }

  }


  "timesTillEnd" should {
    "generate the correct number of times" in {

      assert(
        DateTimeFunctions.timesTillEnd(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          LocalDateTime.of(2016, 12, 19, 0, 0, 0),
          Duration.ofDays(1)
        ) === 1
      )

      assert(
        DateTimeFunctions.timesTillEnd(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          LocalDateTime.of(2016, 12, 19, 0, 0, 0),
          Duration.ofHours(1)
        ) === 24
      )

      assert(
        DateTimeFunctions.timesTillEnd(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          LocalDateTime.of(2016, 12, 19, 0, 0, 0),
          Duration.ofMinutes(30)
        ) === 48
      )

      assert(
        DateTimeFunctions.timesTillEnd(
          LocalDateTime.of(2016, 12, 18, 0, 0, 0),
          LocalDateTime.of(2016, 12, 18, 1, 0, 0),
          Duration.ofMinutes(1)
        ) === 60
      )

    }

  }
}
