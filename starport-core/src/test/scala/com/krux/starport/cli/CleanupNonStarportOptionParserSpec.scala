package com.krux.starport.cli

import org.joda.time.DateTime
import org.scalatest.WordSpec

import com.krux.starport.util.PipelineState


class CleanupNonStarportOptionParserSpec extends WordSpec {
  "CleanupNonStarportOptionParser" when {
    "args is empty" should {
      val args = Array(
        "--excludePrefixes",
        "sp_"
      )

      "return FINISHED as default pipelineState" in {
        val options = CleanupNonStarportOptionParser.parse(args).get
        assert(options.pipelineState === PipelineState.FINISHED)
      }

      "return 2 months ago as default cutoffDate" in {
        val options = CleanupNonStarportOptionParser.parse(args).get
        val cutoffDate = options.cutoffDate
        val expectedDate = DateTime.now.minusMonths(2)

        assert(cutoffDate.getDayOfMonth() === expectedDate.getDayOfMonth())
        assert(cutoffDate.getMonthOfYear() === expectedDate.getMonthOfYear())
        assert(cutoffDate.getYear() === expectedDate.getYear())
      }

      "return false as default dryRun" in {
        val options = CleanupNonStarportOptionParser.parse(args).get
        assert(!options.dryRun)
      }
    }
    "args is valid" should {
      val args = Array(
        "--excludePrefixes",
        "sp_env1_,sp_env2_",
        "--pipelineState",
        "PENDING",
        "--cutoffDate",
        "2017-06-30T00:00:00Z",
        "--dryRun"
      )

      "parse excludePrefixes" in {
        val options = CleanupNonStarportOptionParser.parse(args).get
        assert(options.excludePrefixes === Seq("sp_env1_", "sp_env2_"))
      }

      "parse pipelineState" in {
        val options = CleanupNonStarportOptionParser.parse(args).get
        assert(options.pipelineState === PipelineState.PENDING)
      }

      "parse cutoffDate" in {
        val options = CleanupNonStarportOptionParser.parse(args).get
        val cutoffDate = options.cutoffDate
        val expectedDate = new DateTime("2017-06-30T00:00:00Z")

        assert(cutoffDate.getDayOfMonth() === expectedDate.getDayOfMonth())
        assert(cutoffDate.getMonthOfYear() === expectedDate.getMonthOfYear())
        assert(cutoffDate.getYear() === expectedDate.getYear())
      }

      "parse dryRun" in {
        val options = CleanupNonStarportOptionParser.parse(args).get
        assert(options.dryRun)
      }
    }
    "excludePrefixes is not valid" should {
      val args = Array(
        "--excludePrefixes",
        ""
      )

      "return None" in {
        assert(CleanupNonStarportOptionParser.parse(args) === None)
      }
    }
    "pipelineState is not valid" should {
      val args = Array(
        "--excludePrefixes",
        "sp_",
        "--pipelineState",
        "PROMOTED"
      )

      "return None" in {
        assert(CleanupNonStarportOptionParser.parse(args) === None)
      }
    }
    "cutoffDate is not valid" should {
      val args = Array(
        "--excludePrefixes",
        "sp_",
        "--cutoffDate",
        "9999-99-99T99:99:99Z"
      )

      "return None" in {
        assert(CleanupNonStarportOptionParser.parse(args) === None)
      }
    }
  }

}
