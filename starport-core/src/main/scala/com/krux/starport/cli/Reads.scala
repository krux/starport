package com.krux.starport.cli

import org.joda.time.DateTime
import scopt.Read
import scopt.Read.reads

import com.krux.starport.util.PipelineState

trait Reads {

  implicit val dateTimeRead: Read[DateTime] = reads(new DateTime(_))

  implicit val pipelineStateRead: scopt.Read[PipelineState.State] = scopt.Read.reads(PipelineState.withName)

}
