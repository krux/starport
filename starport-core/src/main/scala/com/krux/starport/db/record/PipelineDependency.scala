package com.krux.starport.db.record

case class PipelineDependency(
  pipelineId: Int,
  dependentPipelineId: Int
)
