package com.krux.starport.metric

import java.util.concurrent.TimeUnit

import scala.util.Random

import com.codahale.metrics.graphite.{Graphite, GraphiteReporter}
import com.codahale.metrics.MetricRegistry

import com.typesafe.config.Config


class MetricSettings(val config: Config) extends Serializable {

  val hosts = config.getStringList("hosts")

  val port = config.getInt("port")

  val metricPrefix = config.getString("prefix")

  def getGraphite: Graphite =
    new Graphite(hosts.get(Random.nextInt(hosts.size)), port)

  def getReporter(registry: MetricRegistry) = GraphiteReporter.forRegistry(registry)
    .prefixedWith(metricPrefix)
    .convertDurationsTo(TimeUnit.SECONDS)
    .build(getGraphite)

}
