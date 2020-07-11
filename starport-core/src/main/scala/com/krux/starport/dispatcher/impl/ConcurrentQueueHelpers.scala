package com.krux.starport.dispatcher.impl

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import com.krux.starport.db.record.ScheduledPipeline

/**
 * This object contains few methods to make the java implementation of ConcurrentLinkedQueue nicer to use
 */
object ConcurrentQueueHelpers {

    implicit class AugmentedConcurrentQueue(q: ConcurrentLinkedQueue[ScheduledPipeline]) {
      /**
       * Empties the queue
       */
      def consumeAll(): Unit = {
        while(!q.isEmpty) {
          q.poll()
        }
      }

      /**
       * Returns all the elements of the queue in an ordered List and removes all the elements from the queue
       * @return
       */
      def retrieveAll() = {
        val elements = q.iterator().asScala.toList
        consumeAll()
        elements
      }
    }

}
