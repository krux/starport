package com.krux.starport.config

import java.net.URL

import scala.collection.JavaConverters._
import scala.collection.{Map => IMap}
import scala.util.Try

import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}

import com.krux.starport.net.StarportURLStreamHandlerFactory


class StarportSettings(val config: Config) extends Serializable {

  val starportJarUrl = config.getString("krux.starport.jar.url")

  val starportNotificationSns = config.getString("krux.starport.notification.sns")

  val metricSettings: Option[Config] =
    Try(config.getConfig("krux.starport.metric.graphite")).toOption

  val jdbc: JdbcConfig = JdbcConfig(config.getConfig("krux.starport.jdbc"))

  val parallel: Int = config.getInt("krux.starport.parallel")

  val maxRetry: Int = config.getInt("hyperion.aws.client.max_retry")

  val maxPipelines: Int = config.getInt("krux.starport.max_pipelines")

  val pipelinePrefix: String = config.getString("krux.starport.prefix")

  val extraEnvs: IMap[String, String] = config
    .getConfig("krux.starport.extra_envs").root.asScala.mapValues { v =>
      assert(v.valueType == ConfigValueType.STRING)
      v.unwrapped.asInstanceOf[String]
    }

  val slackWebhookURL: Option[String] = Try(config.getString("krux.starport.slack_webhook_url")).toOption

  val toEmails: Seq[String] = config.getStringList("krux.starport.notification.email.to").asScala

  val fromEmail: String = config.getString("krux.starport.notification.email.from")
}

object StarportSettings {

  def starportConfigUrl: Option[String] =
    Option(System.getProperty("starport.config.url"))
      .orElse(Option(System.getenv("STARPORT_CONFIG_URL")))

  def getConfig(): Config = {
    StarportURLStreamHandlerFactory.register()

    // for loading external configurations
    starportConfigUrl
      .map(url => ConfigFactory.load(ConfigFactory.parseURL(new URL(url))))
      .getOrElse(ConfigFactory.load("starport"))
  }

  def apply(): StarportSettings = new StarportSettings(getConfig())

}
