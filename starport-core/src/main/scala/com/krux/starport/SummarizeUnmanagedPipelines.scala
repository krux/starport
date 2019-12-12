package com.krux.starport

import com.krux.starport.util.AwsDataPipeline

object SummarizeUnmanagedPipelines extends StarportActivity {

  def run(): Unit = {
    val pipelinesInAws = AwsDataPipeline.listPipelineIds() -- inConsoleManagedPipelineIds()
    val pipelineStatuses = AwsDataPipeline.describePipeline(pipelinesInAws.toSeq: _*)

    logger.info("Unmanaged Pipeline Count By State:")
    pipelineStatuses.values.groupBy(_.pipelineState).mapValues(_.size).toSeq.sortBy(_._2)(Ordering[Int].reverse)
      .foreach { case (state, count) => logger.info(s"${state.getOrElse("Unknown")} -> $count") }

    logger.info("Unmanaged Pipeline Count By Date:")
    pipelineStatuses.values
      .map(status => status.creationTime.flatMap(_.split("T").headOption).getOrElse("Unknown"))
      .groupBy(identity)
      .mapValues(_.size)
      .toSeq
      .sortBy(_._1)(Ordering[String].reverse)
      .foreach { case (date, count) => logger.info(s"$date -> $count") }
  }

  def main(args: Array[String]): Unit = {
    val start = System.nanoTime()
    run()
    val timeSpan = (System.nanoTime - start) / 1E9
    logger.info(s"Done in $timeSpan seconds")
  }

}
