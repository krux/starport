package com.krux.starport.metric

import java.util.concurrent.TimeUnit

import scala.util.Random

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.{MetricRegistry, ScheduledReporter, ConsoleReporter}

import com.typesafe.config.Config


class MetricSettings(val config: Config) extends Serializable {

  val hosts = config.getStringList("hosts")

  val port = config.getInt("port")

  val metricPrefix = config.getString("prefix")

  def getGraphite: Graphite =
    new Graphite(hosts.get(Random.nextInt(hosts.size)), port)

  def getReporter(registry: MetricRegistry): ScheduledReporter = GraphiteReporter.forRegistry(registry)
    .prefixedWith(metricPrefix)
    .convertDurationsTo(MetricSettings.DefaultDuration)
    .build(getGraphite)

}

object MetricSettings {

  final val DefaultDuration = TimeUnit.SECONDS

  def getReporter(configOpt: Option[Config], registry: MetricRegistry): ScheduledReporter =
    configOpt.map(new MetricSettings(_).getReporter(registry))
      .getOrElse(
        ConsoleReporter.forRegistry(registry).convertDurationsTo(DefaultDuration).build()
      )
}
