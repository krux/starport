package com.krux.starport.db

import java.sql.Timestamp
import java.util.Calendar

import com.github.nscala_time.time.Imports.{ DateTime, DateTimeZone }
import slick.jdbc.PostgresProfile.api._

trait DateTimeMapped {

  implicit def mappledDateTime = MappedColumnType.base[DateTime, Timestamp](
    dt => {
      val epoch = dt.getMillis
      val offset = Calendar.getInstance().getTimeZone().getOffset(epoch)
      new Timestamp(epoch - offset)
    },
    ts => new DateTime(ts.getTime).withZoneRetainFields(DateTimeZone.UTC)
  )

}
