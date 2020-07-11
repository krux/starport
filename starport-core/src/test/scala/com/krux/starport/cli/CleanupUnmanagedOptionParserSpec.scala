package com.krux.starport.cli

import java.time.LocalDateTime

import org.scalatest.wordspec.AnyWordSpec

import com.krux.starport.util.PipelineState
import com.krux.starport.util.DateTimeFunctions


class CleanupUnmanagedOptionParserSpec extends AnyWordSpec {
  "CleanupUnmanagedOptionParser" when {
    "args is empty" should {
      val args = Array.empty[String]

      "return empty excludePrefixes" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        assert(options.excludePrefixes === Array.empty[String])
      }

      "return FINISHED as default pipelineState" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        assert(options.pipelineState === PipelineState.FINISHED)
      }

      "return 2 months ago as default cutoffDate" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        val cutoffDate = options.cutoffDate
        val expectedDate = DateTimeFunctions.currentTimeUTC.toLocalDateTime.minusMonths(2)

        assert(cutoffDate.getDayOfMonth() === expectedDate.getDayOfMonth())
        assert(cutoffDate.getMonth() === expectedDate.getMonth())
        assert(cutoffDate.getYear() === expectedDate.getYear())
      }

      "return false as default dryRun" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        assert(!options.dryRun)
      }
    }
    "args is valid" should {
      val args = Array(
        "--excludePrefix",
        "sp_",
        "--pipelineState",
        "PENDING",
        "--cutoffDate",
        "2017-06-30T00:00:00Z",
        "--dryRun"
      )

      "parse excludePrefixes" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        assert(options.excludePrefixes === Seq("sp_"))
      }

      "parse pipelineState" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        assert(options.pipelineState === PipelineState.PENDING)
      }

      "parse cutoffDate" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        val cutoffDate = options.cutoffDate
        val expectedDate = LocalDateTime.of(2017, 6, 30, 0, 0, 0)

        assert(cutoffDate.getDayOfMonth() === expectedDate.getDayOfMonth())
        assert(cutoffDate.getMonth() === expectedDate.getMonth())
        assert(cutoffDate.getYear() === expectedDate.getYear())
      }

      "parse dryRun" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        assert(options.dryRun)
      }
    }
    "multiple excludePrefixes" should {
      val args = Array(
        "--excludePrefix",
        "sp_env1_",
        "--excludePrefix",
        "sp_env2_"
      )

      "parse multiple excludePrefixes" in {
        val options = CleanupUnmanagedOptionParser.parse(args).get
        assert(options.excludePrefixes === Seq("sp_env1_", "sp_env2_"))
      }
    }
    "excludePrefix is not valid" should {
      val args = Array(
        "--excludePrefix",
        ""
      )

      "return None" in {
        assert(CleanupUnmanagedOptionParser.parse(args) === None)
      }
    }
    "pipelineState is not valid" should {
      val args = Array(
        "--pipelineState",
        "PROMOTED"
      )

      "return None" in {
        assert(CleanupUnmanagedOptionParser.parse(args) === None)
      }
    }
    "cutoffDate is not valid" should {
      val args = Array(
        "--cutoffDate",
        "9999-99-99T99:99:99Z"
      )

      "return None" in {
        assert(CleanupUnmanagedOptionParser.parse(args) === None)
      }
    }
  }

}
