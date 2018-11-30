package com.krux.starport.util

import scala.annotation.tailrec
import scala.language.implicitConversions

import com.github.nscala_time.time.Imports._

import com.krux.hyperion.expression.{ Duration, Year, Month, Week, Day, Hour, Minute }

trait DateTimeFunctions {

  final val DateTimeFormat = "yyyy-MM-dd'T'HH:mm:ss'Z'"

  def nextWholeHour(dt: DateTime): DateTime = {
    val nextHour = dt + 1.hour
    nextHour.withTime(nextHour.getHourOfDay, 0, 0, 0)
  }

  /**
   * Number of times from start (inclusive) to end (exlcusive)
   */
  def timesTillEnd(start: DateTime, end: DateTime, period: Period): Int = {

    @tailrec
    def timesTillEndRec(start: DateTime, end: DateTime, period: Period, accu: Int = 0): Int =
      if (start >= end) accu
      else timesTillEndRec(start + period, end, period, accu + 1)

    timesTillEndRec(start, end, period)

  }

  /**
   * Get the next run time pass the current time
   */
  @tailrec
  final def nextRunTime(previousNextRunTime: DateTime, period: Period, currentEndTime: DateTime): DateTime = {
    if (previousNextRunTime >= currentEndTime) previousNextRunTime
    else nextRunTime(previousNextRunTime + period, period, currentEndTime)
  }

  /**
   * Get the run the that just before now but after the start
   */
  @tailrec
  final def previousRunTime(previousNextRunTime: DateTime, period: Period, currentTime: DateTime): DateTime = {
    if (previousNextRunTime <= currentTime) previousNextRunTime
    else previousRunTime(previousNextRunTime - period, period, currentTime)
  }

  implicit def duration2Period(duration: Duration): Period = duration match {
    case Year(n) => Period.years(n)
    case Month(n) => Period.months(n)
    case Week(n) => Period.weeks(n)
    case Day(n) => Period.days(n)
    case Hour(n) => Period.hours(n)
    case Minute(m) => Period.minutes(m)
  }

  implicit object DateTimeOrdering extends math.Ordering[DateTime] {
    def compare(x: DateTime, y: DateTime) = x.compareTo(y)
  }

}

object DateTimeFunctions extends DateTimeFunctions
