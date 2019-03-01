package com.krux.starport.util.notification

import com.krux.starport.config.StarportSettings
import com.krux.starport.db.record.Pipeline

trait Notification {
	def send(summary: String, message: String, pipeline: Pipeline)(implicit conf: StarportSettings): String
}

object Notification {
	private val validEmail = """^([a-zA-Z0-9.!#$%&’'*+/=?^_`{|}~-]+)@([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*)$"""
	private val validSNSarn = """^arn:aws:sns:([a-z0-9-*]+):([0-9*]+):([a-zA-Z0-9-_]+)$"""

	private lazy val emailNotification = new EmailNotification
	private lazy val snsNotification = new SNSNotification

	private val notificationFinder = Map(
		validEmail -> emailNotification,
		validSNSarn -> snsNotification
	)

	def isNameValid(name: String): Boolean = {
		notificationFinder.exists(p => name.matches(p._1))
	}

	/**
		* Finds the notification medium for the relevant owner type. Generates exception in case of an invalid owner type
		* as that should have been caught during argument parsing for owner. Default notification is email.
		*/
	def apply(name: Option[String]): Notification = name.map { n =>
		notificationFinder.
			find(p => n.matches(p._1)).getOrElse(throw new IllegalArgumentException("Invalid owner type"))._2
	}.getOrElse(emailNotification)
}
