package com.krux.starport.util

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalAmount}
import java.time.{LocalDateTime, Period, Duration, ZonedDateTime, ZoneOffset}

import scala.annotation.tailrec
import scala.language.implicitConversions

import com.krux.hyperion.expression.{Duration => HyperionDuration, Year, Month, Week, Day, Hour, Minute}

trait DateTimeFunctions {

  final val DateTimeFormatPattern = "yyyy-MM-dd'T'HH:mm:ss'Z'"
  final val DateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")

  def currentTimeUTC(): ZonedDateTime = ZonedDateTime.now.withZoneSameInstant(ZoneOffset.UTC)

  def nextWholeHour(dt: LocalDateTime): LocalDateTime =
    dt.plusHours(1).truncatedTo(ChronoUnit.HOURS)

  /**
   * Number of times from start (inclusive) to end (exlcusive)
   */
  def timesTillEnd(start: LocalDateTime, end: LocalDateTime, ta: TemporalAmount): Int = {

    @tailrec
    def timesTillEndRec(start: LocalDateTime, end: LocalDateTime, ta: TemporalAmount, accu: Int = 0): Int =
      if (!start.isBefore(end)) accu
      else timesTillEndRec(start.plus(ta), end, ta, accu + 1)

    timesTillEndRec(start, end, ta)

  }

  /**
   * Get the next run time pass the current time
   */
  @tailrec
  final def nextRunTime(previousNextRunTime: LocalDateTime, ta: TemporalAmount, currentEndTime: LocalDateTime): LocalDateTime = {
    if (!previousNextRunTime.isBefore(currentEndTime)) previousNextRunTime
    else nextRunTime(previousNextRunTime.plus(ta), ta, currentEndTime)
  }

  /**
   * Get the run the that just before now but after the start
   */
  @tailrec
  final def previousRunTime(previousNextRunTime: LocalDateTime, ta: TemporalAmount, currentTime: LocalDateTime): LocalDateTime = {
    if (!previousNextRunTime.isAfter(currentTime)) previousNextRunTime
    else previousRunTime(previousNextRunTime.minus(ta), ta, currentTime)
  }

  implicit def duration2Period(duration: HyperionDuration): TemporalAmount = duration match {
    case Year(n) => Period.ofYears(n)
    case Month(n) => Period.ofMonths(n)
    case Week(n) => Period.ofWeeks(n)
    case Day(n) => Period.ofDays(n)
    case Hour(n) => Duration.ofHours(n)
    case Minute(m) => Duration.ofMinutes(m)
  }

  implicit object LocalDateTimeOrdering extends math.Ordering[LocalDateTime] {
    def compare(x: LocalDateTime, y: LocalDateTime) = x.compareTo(y)
  }

}

object DateTimeFunctions extends DateTimeFunctions
