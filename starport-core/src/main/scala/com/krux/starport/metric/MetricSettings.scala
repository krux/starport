package com.krux.starport.metric

import java.util.concurrent.TimeUnit

import scala.util.Random

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{MetricRegistry, ScheduledReporter, ConsoleReporter}
//import io.github.azagniotov.metrics.reporter.cloudwatch.CloudWatchReporter

import com.typesafe.config.Config

sealed trait MetricSettings extends Serializable {

}

case object GraphiteReporterSettings extends MetricSettings {

  final val DefaultDuration = TimeUnit.SECONDS

  def getReporter(configOpt: Option[Config], registry: MetricRegistry): ScheduledReporter =
    configOpt.map(GraphiteReporterSettingsImpl(_).getReporter(registry))
      .getOrElse(
        ConsoleReporter.forRegistry(registry).convertDurationsTo(DefaultDuration).build()
      )

  private case class GraphiteReporterSettingsImpl(config: Config) extends MetricSettings {

    val hosts = config.getStringList("hosts")

    val port = config.getInt("port")

    val metricPrefix = config.getString("prefix")

    def getGraphite: Graphite =
      new Graphite(hosts.get(Random.nextInt(hosts.size)), port)

    def getReporter(registry: MetricRegistry): ScheduledReporter = GraphiteReporter.forRegistry(registry)
      .prefixedWith(metricPrefix)
      .convertDurationsTo(GraphiteReporterSettings.DefaultDuration)
      .build(getGraphite)

  }

}
