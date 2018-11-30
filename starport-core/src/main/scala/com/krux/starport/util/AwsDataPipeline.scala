package com.krux.starport.util

import scala.annotation.tailrec
import scala.collection.JavaConverters._

import com.amazonaws.services.datapipeline.DataPipelineClientBuilder
import com.amazonaws.services.datapipeline.model._
import com.krux.hyperion.client.AwsClient
import com.krux.starport.Logging


trait AwsDataPipeline extends Retry with Logging {

  override val logger = super[Logging].logger

  final val AwsPipelineQueryLimit = 25

  /**
   * Returns a map of awsId and it's status
   */
  def describePipeline(awsIds: String*): Map[String, PipelineStatus] = {
    val client = DataPipelineClientBuilder.defaultClient()

    awsIds
      .flatMap { awsId =>
        try {
          // note that we cannot group aws ids easily as some of the aws pipeline might not be there
          val descReq = new DescribePipelinesRequest().withPipelineIds(awsId)
          val descResult = client.describePipelines(descReq).retry()
          descResult.getPipelineDescriptionList().asScala
            .map(desc => desc.getPipelineId() -> PipelineStatus(desc))
        } catch {
          case e: PipelineNotFoundException => List.empty
          case e: PipelineDeletedException => List.empty
        }
      }
      .toMap

  }

  /**
    * Returns a list of all pipeline Ids
    */
  def listPipelineIds(): Set[String] = {
    @tailrec
    def listPipelineIdsRecursively(
        pipelineIds: Set[String] = Set.empty,
        request: ListPipelinesRequest = new ListPipelinesRequest()
      ): Set[String] = {

      val awsClient = AwsClient.getClient()
      val response = awsClient.listPipelines(request).retry()
      val thesePipelineIds = response.getPipelineIdList
        .asScala
        .map(_.getId)
        .toSet

      if (response.getHasMoreResults)
        listPipelineIdsRecursively(
          pipelineIds ++ thesePipelineIds,
          new ListPipelinesRequest().withMarker(response.getMarker)
        )
      else
        pipelineIds

    }

    listPipelineIdsRecursively()
  }

  def deletePipelines(awsIds: Set[String]): Unit = {
    val client = DataPipelineClientBuilder.defaultClient()

    awsIds.foreach { awsId =>
        val delReq = new DeletePipelineRequest().withPipelineId(awsId)
        client.deletePipeline(delReq).retry()
        logger.info(s"Deleted pipeline with AWS ID $awsId")
      }
  }

}

object AwsDataPipeline extends AwsDataPipeline
