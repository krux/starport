package com.krux.starport.metric

import java.util.concurrent.TimeUnit

import scala.util.Random
import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{ConsoleReporter, MetricFilter, MetricRegistry, ScheduledReporter}
import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter
import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter.Percentile
import com.amazonaws.regions.Regions
import com.amazonaws.services.cloudwatch.model.StandardUnit
import com.amazonaws.services.cloudwatch.{AmazonCloudWatchAsync, AmazonCloudWatchAsyncClientBuilder}
import com.typesafe.config.Config

/**
 * A service trait that is used by private case class constructors.
 */
trait MetricSettingsImpl extends Serializable {

  def getReporter(registry: MetricRegistry): ScheduledReporter

}

/**
 * A sealed trait for metric engine settings. We want this to be sealed as there should only be a finite number of
 * reporting engine options available.
 */
sealed trait MetricSettings {

  /** Set the default duration to be in seconds */
  final val DefaultDuration = TimeUnit.SECONDS

  def getReporter(config: Config, registry: MetricRegistry): ScheduledReporter = this match {
      case GraphiteReporterSettings => GraphiteReporterSettings(config).getReporter(registry)
      case CloudWatchReporterSettings => CloudWatchReporterSettings(config).getReporter(registry)
  }

  def getDefaultReporter(registry: MetricRegistry): ScheduledReporter = ConsoleReporter
    .forRegistry(registry)
    .convertDurationsTo(DefaultDuration)
    .build()

}

final case object GraphiteReporterSettings extends MetricSettings {

  def apply(config: Config): MetricSettingsImpl = GraphiteReporterSettingsImpl(config)

  private case class GraphiteReporterSettingsImpl(config: Config) extends MetricSettingsImpl {

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

}

final case object CloudWatchReporterSettings extends MetricSettings {

  def apply(config: Config): MetricSettingsImpl = CloudWatchReporterSettingsImpl(config)

  private case class CloudWatchReporterSettingsImpl(config: Config) extends MetricSettingsImpl {

    val awsRegion: Regions = Regions.fromName(config.getString("region"))
    val prefix = config.getString("prefix")
    val configGroup = config.getString("group")
    val configModule = config.getString("module")

    val environment = s"Environment=${prefix},"
    val group = s"Group=${configGroup},"
    val module = s"Module=${configModule}"
    //val name = ???
    //val stack = ???
    //val terraform = ???

    val dimensionsList = List[String](
      environment,
      group,
      module
    //  name,
    //  stack,
    //  terraform
    ).reduceLeft(_++_)

    val awsCloudWatchAsync: AmazonCloudWatchAsync = AmazonCloudWatchAsyncClientBuilder
      .standard()
      .withRegion(awsRegion)
      .build()

    def getReporter(metricRegistry: MetricRegistry): CloudWatchReporter = CloudWatchReporter
      .forRegistry(metricRegistry, awsCloudWatchAsync, "STARPORT")
      .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS)
      .filter(MetricFilter.ALL)
      .withPercentiles(Percentile.P75, Percentile.P99)
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
      .withGlobalDimensions(environment)
      .build()

  }

}


