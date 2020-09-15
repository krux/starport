package com.krux.starport.metric

import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.util.Random
import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.model.StandardUnit
import com.amazonaws.services.cloudwatch.{AmazonCloudWatchAsync, AmazonCloudWatchAsyncClientBuilder}
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{ConsoleReporter, MetricFilter, MetricRegistry, ScheduledReporter}
import com.typesafe.config.Config

sealed trait MetricSettings {

  /** Set the default duration to be in seconds */
  final val DefaultDuration = TimeUnit.SECONDS

  def getReporter(registry: MetricRegistry): ScheduledReporter

}

final case object DefaultConsoleReporterSettings extends MetricSettings {

  def getReporter(registry: MetricRegistry): ScheduledReporter = ConsoleReporter
    .forRegistry(registry)
    .convertDurationsTo(DefaultDuration)
    .build()

}


final case class GraphiteReporterSettings(config: Config) extends MetricSettings {

  val hosts = config.getStringList("hosts")

  val port = config.getInt("port")

  val metricPrefix = config.getString("prefix")

  def getGraphite: Graphite =
    new Graphite(hosts.get(Random.nextInt(hosts.size)), port)

  def getReporter(registry: MetricRegistry): ScheduledReporter = GraphiteReporter
    .forRegistry(registry)
    .prefixedWith(metricPrefix)
    .convertDurationsTo(DefaultDuration)
    .build(getGraphite)

}

final case class CloudWatchReporterSettings(config: Config) extends MetricSettings {

  val awsRegion: Regions = Regions.fromName(config.getString("region"))

  val dimensions = config
    .getObject("Dimensions")
    .asScala
    .map { case (k, v) => k + "=" + v.unwrapped.toString }
    .toList

  val awsCloudWatchAsyncClient: AmazonCloudWatchAsync = AmazonCloudWatchAsyncClientBuilder
    .standard()
    .withRegion(awsRegion)
    .build()

  def getReporter(metricRegistry: MetricRegistry): CloudWatchReporter = CloudWatchReporter
    .forRegistry(metricRegistry, awsCloudWatchAsyncClient, "Starport")
    .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
    .filter(MetricFilter.ALL)
    .withPercentiles(P75, P99)
    .withOneMinuteMeanRate()
    .withFiveMinuteMeanRate()
    .withFifteenMinuteMeanRate()
    .withMeanRate()
    .withArithmeticMean()
    .withStdDev()
    .withStatisticSet()
    .withZeroValuesSubmission()
    .withReportRawCountValue()
    .withHighResolution()
    .withMeterUnitSentToCW(StandardUnit.Bytes)
    .withGlobalDimensions(dimensions: _*)
    .build()

}
