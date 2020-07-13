package com.krux.starport.cli

import java.time.{LocalDateTime, ZonedDateTime, ZoneOffset}

import scopt.Read
import scopt.Read.reads

import com.krux.starport.util.PipelineState

trait Reads {

  implicit val dateTimeRead: Read[LocalDateTime] = reads { r =>
    ZonedDateTime.parse(r).withZoneSameInstant(ZoneOffset.UTC).toLocalDateTime()
  }

  implicit val pipelineStateRead: scopt.Read[PipelineState.State] = scopt.Read.reads(PipelineState.withName)

}
