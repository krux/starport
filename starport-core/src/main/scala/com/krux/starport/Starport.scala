package com.krux.starport

import com.github.nscala_time.time.Imports.{DateTime, DateTimeZone}

import com.krux.hyperion.action.SnsAlarm
import com.krux.hyperion.activity.{JarActivity, MainClass}
import com.krux.hyperion.adt.HString
import com.krux.hyperion.common.S3Uri
import com.krux.hyperion.expression.RunnableObject
import com.krux.hyperion.Implicits._
import com.krux.hyperion.resource.Ec2Resource
import com.krux.hyperion.{DataPipelineDef, Schedule, HyperionCli, HyperionContext}
import com.krux.starport.config.StarportSettings
import com.krux.starport.util.DateTimeFunctions

/**
 * The job server is simply a jar activity
 */
object Starport extends DataPipelineDef with HyperionCli {

  private val starportSettings = StarportSettings()

  override implicit lazy val hc = new HyperionContext(starportSettings.config)

  lazy val jarLocation = S3Uri(starportSettings.starportJarUrl)

  val schedulerClass: MainClass = com.krux.starport.StartScheduledPipelines
  val cleanupClass: MainClass = com.krux.starport.CleanupExistingPipelines

  def currentHour = DateTime.now.withZone(DateTimeZone.UTC).getHourOfDay()

  def schedule = Schedule.cron.startTodayAt(currentHour, 0, 0).every(1.hour)

  def workflow = {

    val alarm = SnsAlarm(starportSettings.starportNotificationSns)
      .withSubject(s"[Hyperion] ${pipelineName} failed")

    val ec2 = Ec2Resource()

    val scheduledStart = RunnableObject.ScheduledStartTime.format(DateTimeFunctions.DateTimeFormat)
    val scheduledEnd = RunnableObject.ScheduledEndTime.format(DateTimeFunctions.DateTimeFormat)
    val actualStart = RunnableObject.ActualStartTime.format(DateTimeFunctions.DateTimeFormat)

    val schedulerArgs = Seq[HString](
        "--start", scheduledStart,
        "--end", scheduledEnd,
        "--actual-start", actualStart
      )

    val jvmOption = StarportSettings.starportConfigUrl.map(url => s"-Dstarport.config.url=$url")

    val baseStartPipelines = JarActivity(jarLocation)(ec2)
      .named(schedulerClass.simpleName)
      .withMainClass(schedulerClass)
      .withArguments(schedulerArgs: _*)
      .onFail(alarm)

    val startPipelines = jvmOption.map(baseStartPipelines.withOptions(_)).getOrElse(baseStartPipelines)

    val baseCleanupPipelines = JarActivity(jarLocation)(ec2)
      .named(cleanupClass.simpleName)
      .withMainClass(cleanupClass)
      .onFail(alarm)

    val cleanupPipelines = jvmOption.map(baseCleanupPipelines.withOptions(_)).getOrElse(baseCleanupPipelines)

    startPipelines ~> cleanupPipelines

  }

}
