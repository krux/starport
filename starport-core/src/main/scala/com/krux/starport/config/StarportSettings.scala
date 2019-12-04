package com.krux.starport.config

import java.net.URL

import scala.collection.JavaConverters._
import scala.collection.{Map => IMap}
import scala.util.{Failure, Success, Try}
import com.typesafe.config.{Config, ConfigFactory, ConfigValueType}
import com.amazonaws.regions.Regions
import com.krux.starport.metric.{CloudWatchReporterSettings, GraphiteReporterSettings, MetricSettings}
import com.krux.starport.net.StarportURLStreamHandlerFactory

class StarportSettings(val config: Config) extends Serializable {

  val region = Try(config.getString("krux.starport.region")).toOption.map(Regions.fromName)

  val starportJarUrl = config.getString("krux.starport.jar.url")

  val starportNotificationSns = config.getString("krux.starport.notification.sns")

  val metricsEngine: MetricSettings =
    Try(config.getString("krux.starport.metric.engine")) match {
      case Success(engine) => engine match {
        case s if (s == "graphite") => GraphiteReporterSettings
        case s if (s == "cloudwatch") => CloudWatchReporterSettings
      }
      case Failure(_) => throw new InstantiationException("No metrics engine specified.")
    }

  val metricConfig: Option[Config] = metricsEngine match {
      case GraphiteReporterSettings => Try(config.getConfig("krux.starport.metric.graphite")).toOption
      case CloudWatchReporterSettings => Try(config.getConfig("krux.starport.metric.cloudwatch")).toOption
   }

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

  val snsTopicARN: String = if (config.hasPath("krux.starport.notification.sns_owner")) {
    config.getString("krux.starport.notification.sns_owner")
  }
  else {
    config.getString("krux.starport.notification.sns")
  }
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
