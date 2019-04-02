package com.krux.starport.util.notification

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline

trait Notification {
  def send(summary: String, message: String, pipeline: Pipeline)(implicit conf: StarportSettings): String
}
