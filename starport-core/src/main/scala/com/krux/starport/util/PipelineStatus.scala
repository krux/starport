package com.krux.starport.util

import scala.collection.JavaConverters._

import com.amazonaws.services.datapipeline.model.PipelineDescription

case class PipelineStatus(
  awsId: String,
  healthStatus: Option[String],
  pipelineState: Option[PipelineState.State],
  creationTime: Option[String]
)

object PipelineStatus {

  final val HealthStatusKey = "@healthStatus"
  final val PipelineStateKey = "@pipelineState"
  final val CreationTimeKey = "@creationTime"

  def apply(desc: PipelineDescription) = {
    val awsId = desc.getPipelineId()
    val fields = desc.getFields().asScala
      .map { f => f.getKey -> f.getStringValue }
      .toMap

    new PipelineStatus(
      awsId,
      fields.get(HealthStatusKey),
      fields.get(PipelineStateKey).map(PipelineState.withName),
      fields.get(CreationTimeKey)
    )
  }

}
