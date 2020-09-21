package com.krux.starport.metric

import java.util._
import java.util.concurrent.{ConcurrentHashMap, Future, TimeUnit}

import scala.collection.JavaConverters._
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync
import com.amazonaws.services.cloudwatch.model.{
  Dimension,
  MetricDatum,
  PutMetricDataRequest,
  PutMetricDataResult,
  StandardUnit,
  StatisticSet
}
import com.codahale.metrics.{
  Clock,
  Counter,
  Counting,
  Gauge,
  Histogram,
  Meter,
  Metered,
  MetricFilter,
  MetricRegistry,
  ScheduledReporter,
  Snapshot,
  Timer
}
import org.slf4j.{Logger, LoggerFactory}
import CloudWatchReporter._

/**
 * Reports metrics to <a href="http://aws.amazon.com/cloudwatch/">Amazon's CloudWatch</a> periodically.
 * <p>
 * Use {@link CloudWatchReporter.Builder} to construct instances of this class. The {@link CloudWatchReporter.Builder}
 * allows to configure what aggregated metrics will be reported as a single {@link MetricDatum} to CloudWatch.
 * <p>
 * There are a bunch of {@code with*} methods that provide a sufficient fine-grained control over what metrics
 * should be reported
 */
case class CloudWatchReporter(builder: Builder)
    extends ScheduledReporter(
      builder.metricRegistry,
      "coda-hale-metrics-cloud-watch-reporter",
      builder.metricFilter,
      builder.rateUnit,
      builder.durationUnit
    ) {

  /**
   * We only submit the difference in counters since the last submission. This way we don't have to reset
   * the counters within this application.
   */
  private val lastPolledCounts: Map[Counting, Long] = new ConcurrentHashMap()

  private val namespace: String = builder.namespace

  private val cloudWatchAsyncClient: AmazonCloudWatchAsync = builder.cloudWatchAsyncClient

  private val rateUnit: StandardUnit = builder.cwRateUnit

  private val durationUnit: StandardUnit = builder.cwDurationUnit

  private val highResolution: Boolean = builder.highResolution

  override def report(
    gauges: java.util.SortedMap[String, Gauge[_]],
    counters: java.util.SortedMap[String, Counter],
    histograms: java.util.SortedMap[String, Histogram],
    meters: java.util.SortedMap[String, Meter],
    timers: java.util.SortedMap[String, Timer]
  ): Unit = {

    if (builder.dryRun) {
      logger.warn("** Reporter is running in 'DRY RUN' mode **")
    }
    try {
      val metricData: List[MetricDatum] = new ArrayList[MetricDatum](
        gauges.size + counters.size + 10 * histograms.size + 10 * timers.size
      )
      gauges.forEach { case (k: String, v: Gauge[_]) => processGauge(k, v, metricData) }
      counters.forEach { case (k: String, v: Counter) => processCounter(k, v, metricData) }
      histograms.forEach {
        case (k: String, v: Histogram) => {
          processHistogram(k, v, metricData)
          processCounter(k, v, metricData)
        }
      }

      meters.forEach {
        case (k: String, v: Meter) => {
          processMeter(k, v, metricData)
          processCounter(k, v, metricData)
        }
      }
      timers.forEach {
        case (k: String, v: Timer) => {
          processTimer(k, v, metricData)
          processMeter(k, v, metricData)
          processCounter(k, v, metricData)
        }
      }

      val metricDataPartitions: Collection[List[MetricDatum]] =
        CollectionsUtils.partition(metricData, maximumDatumsPerRequest)
      val cloudWatchFutures: List[Future[PutMetricDataResult]] =
        new ArrayList[Future[PutMetricDataResult]](metricData.size)

      metricDataPartitions.forEach(partition => {
        val putMetricDataRequest: PutMetricDataRequest = new PutMetricDataRequest()
          .withNamespace(namespace)
          .withMetricData(partition)
        if (builder.dryRun) {
          if (logger.isDebugEnabled) {
            logger.debug("Dry run - constructed PutMetricDataRequest: {}", putMetricDataRequest)
          }
        } else {
          cloudWatchFutures.add(cloudWatchAsyncClient.putMetricDataAsync(putMetricDataRequest))
        }
      })

      cloudWatchFutures.forEach(cloudWatchFuture => {
        try cloudWatchFuture.get
        catch {
          case e: Exception =>
            logger.error(
              "Error reporting metrics to CloudWatch. The data in this CloudWatch API request " +
                "may have been discarded, did not make it to CloudWatch.",
              e
            )

        }
      })

      if (logger.isDebugEnabled) {
        logger.debug(
          s"Sent ${metricData.size} datums to CloudWatch. Namespace: ${namespace}, metric data ${metricData}."
        )
      }
    } catch {
      case e: RuntimeException =>
        logger.error("Error marshalling CloudWatch metrics.", e)

    }
  }

  override def stop(): Unit = {
    try super.stop()
    catch {
      case e: Exception => logger.error("Error when stopping the reporter.", e)

    } finally if (!builder.dryRun) {
      try cloudWatchAsyncClient.shutdown()
      catch {
        case e: Exception =>
          logger.error("Error shutting down AmazonCloudWatchAsync", e)

      }
    }
  }

  private def processGauge[_ <: Numeric[_]](
    metricName: String,
    gauge: Gauge[_],
    metricData: List[MetricDatum]
  ): Unit = {
    Optional
      .ofNullable(gauge.getValue)
      .ifPresent(value =>
        stageMetricDatum(
          true,
          metricName,
          value.asInstanceOf[Double],
          StandardUnit.None,
          dimensionGauge,
          metricData
        )
      )
  }

  private def processCounter(
    metricName: String,
    counter: Counting,
    metricData: List[MetricDatum]
  ): Unit = {
    val currentCount: Long = counter.getCount
    var lastCount: java.lang.Long = lastPolledCounts.get(counter)
    lastPolledCounts.put(counter, currentCount)
    if (lastCount == null) {
      lastCount = 0L
    }
    var reportValue: Long = 0L
    reportValue =
      if (builder.reportRawCountValue) currentCount
      else currentCount - lastCount
    stageMetricDatum(true, metricName, reportValue, StandardUnit.Count, dimensionCount, metricData)
  }

  /**
   * The rates of {@link Metered} are reported after being converted using the rate factor, which is deduced from
   * the set rate unit
   *
   * @see Timer#getSnapshot
   * @see #getRateUnit
   * @see #convertRate(double)
   */
  private def processMeter(
    metricName: String,
    meter: Metered,
    metricData: List[MetricDatum]
  ): Unit = {
    val formattedRate: String = String.format("-rate [per-%s]", getRateUnit)
    stageMetricDatum(
      builder.oneMinuteMeanRate,
      metricName,
      convertRate(meter.getOneMinuteRate),
      rateUnit,
      "1-min-mean" + formattedRate,
      metricData
    )
    stageMetricDatum(
      builder.fiveMinuteMeanRate,
      metricName,
      convertRate(meter.getFiveMinuteRate),
      rateUnit,
      "5-min-mean" + formattedRate,
      metricData
    )
    stageMetricDatum(
      builder.fifteenMinuteMeanRate,
      metricName,
      convertRate(meter.getFifteenMinuteRate),
      rateUnit,
      "15-min-mean" + formattedRate,
      metricData
    )
    stageMetricDatum(
      builder.meanRate,
      metricName,
      convertRate(meter.getMeanRate),
      rateUnit,
      "mean" + formattedRate,
      metricData
    )
  }

  /**
   * The {@link Snapshot} values of {@link Timer} are reported as {@link StatisticSet} after conversion. The
   * conversion is done using the duration factor, which is deduced from the set duration unit.
   * <p>
   * Please note, the reported values submitted only if they show some data (greater than zero) in order to:
   * <p>
   * 1. save some money
   * 2. prevent com.amazonaws.services.cloudwatch.model.InvalidParameterValueException if empty {@link Snapshot}
   * is submitted
   * <p>
   * If {@link Builder#withZeroValuesSubmission()} is {@code true}, then all values will be submitted
   *
   * @see Timer#getSnapshot
   * @see #getDurationUnit
   * @see #convertDuration(double)
   */
  private def processTimer(
    metricName: String,
    timer: Timer,
    metricData: List[MetricDatum]
  ): Unit = {
    val snapshot: Snapshot = timer.getSnapshot
    if (builder.zeroValuesSubmission || snapshot.size > 0) {
      for (percentile <- builder.percentiles) {
        val convertedDuration: Double = convertDuration(snapshot.getValue(percentile.quantile))
        stageMetricDatum(
          true,
          metricName,
          convertedDuration,
          durationUnit,
          percentile.description,
          metricData
        )
      }
    }
// prevent empty snapshot from causing InvalidParameterValueException
    if (snapshot.size > 0) {
      val formattedDuration: String =
        String.format(" [in-%s]", getDurationUnit)
      stageMetricDatum(
        builder.arithmeticMean,
        metricName,
        convertDuration(snapshot.getMean),
        durationUnit,
        dimensionSnapshotMean + formattedDuration,
        metricData
      )
      stageMetricDatum(
        builder.stdDev,
        metricName,
        convertDuration(snapshot.getStdDev),
        durationUnit,
        dimensionSnapshotStdDev + formattedDuration,
        metricData
      )
      stageMetricDatumWithConvertedSnapshot(
        builder.statisticSet,
        metricName,
        snapshot,
        durationUnit,
        metricData
      )
    }
  }

  /**
   * The {@link Snapshot} values of {@link Histogram} are reported as {@link StatisticSet} raw. In other words, the
   * conversion using the duration factor does NOT apply.
   * <p>
   * Please note, the reported values submitted only if they show some data (greater than zero) in order to:
   * <p>
   * 1. save some money
   * 2. prevent com.amazonaws.services.cloudwatch.model.InvalidParameterValueException if empty {@link Snapshot}
   * is submitted
   * <p>
   * If {@link Builder#withZeroValuesSubmission()} is {@code true}, then all values will be submitted
   *
   * @see Histogram#getSnapshot
   */
  private def processHistogram(
    metricName: String,
    histogram: Histogram,
    metricData: List[MetricDatum]
  ): Unit = {
    val snapshot: Snapshot = histogram.getSnapshot
    if (builder.zeroValuesSubmission || snapshot.size > 0) {
      for (percentile <- builder.percentiles) {
        val value: Double = snapshot.getValue(percentile.quantile)
        stageMetricDatum(
          true,
          metricName,
          value,
          StandardUnit.None,
          percentile.description,
          metricData
        )
      }
    }
// prevent empty snapshot from causing InvalidParameterValueException
    if (snapshot.size > 0) {
      stageMetricDatum(
        builder.arithmeticMean,
        metricName,
        snapshot.getMean,
        StandardUnit.None,
        dimensionSnapshotMean,
        metricData
      )
      stageMetricDatum(
        builder.stdDev,
        metricName,
        snapshot.getStdDev,
        StandardUnit.None,
        dimensionSnapshotStdDev,
        metricData
      )
      stageMetricDatumWithRawSnapshot(
        builder.statisticSet,
        metricName,
        snapshot,
        StandardUnit.None,
        metricData
      )
    }
  }

  /**
   * Please note, the reported values submitted only if they show some data (greater than zero) in order to:
   * <p>
   * 1. save some money
   * 2. prevent com.amazonaws.services.cloudwatch.model.InvalidParameterValueException if empty {@link Snapshot}
   * is submitted
   * <p>
   * If {@link Builder#withZeroValuesSubmission()} is {@code true}, then all values will be submitted
   */
  private def stageMetricDatum(
    metricConfigured: Boolean,
    metricName: String,
    metricValue: Double,
    standardUnit: StandardUnit,
    dimensionValue: String,
    metricData: List[MetricDatum]
  ): Unit = {
// Only submit metrics that show some data, so let's save some money
    if (metricConfigured && (builder.zeroValuesSubmission || metricValue > 0)) {
      val dimensionedName: DimensionedName = DimensionedName.decode(metricName)
      val dimensions: Set[Dimension] = new LinkedHashSet[Dimension](builder.globalDimensions)

      dimensions.add(
        new Dimension()
          .withName(dimensionNameType)
          .withValue(dimensionValue)
      )

      dimensions.addAll(dimensionedName.getDimensions().asJavaCollection)

      metricData.add(
        new MetricDatum()
          .withTimestamp(new Date(builder.clock.getTime))
          .withValue(cleanMetricValue(metricValue))
          .withMetricName(dimensionedName.name)
          .withDimensions(dimensions)
          .withStorageResolution(
            if (highResolution) highResolutionFrequency
            else standardResolutionFrequency
          )
          .withUnit(standardUnit)
      )
    }
  }

  private def stageMetricDatumWithConvertedSnapshot(
    metricConfigured: Boolean,
    metricName: String,
    snapshot: Snapshot,
    standardUnit: StandardUnit,
    metricData: List[MetricDatum]
  ): Unit = {

    if (metricConfigured) {
      val dimensionedName: DimensionedName = DimensionedName.decode(metricName)
      val scaledSum: Double = convertDuration(snapshot.getValues.toStream.sum)

      val statisticSet: StatisticSet = new StatisticSet()
        .withSum(scaledSum)
        .withSampleCount(snapshot.size.toDouble)
        .withMinimum(convertDuration(snapshot.getMin))
        .withMaximum(convertDuration(snapshot.getMax))

      val dimensions: Set[Dimension] =
        new LinkedHashSet[Dimension](builder.globalDimensions)
      dimensions.add(
        new Dimension()
          .withName(dimensionNameType)
          .withValue(dimensionSnapshotSummary)
      )

      dimensions.addAll(dimensionedName.getDimensions.asJavaCollection)
      metricData.add(
        new MetricDatum()
          .withTimestamp(new Date(builder.clock.getTime))
          .withMetricName(dimensionedName.name)
          .withDimensions(dimensions)
          .withStatisticValues(statisticSet)
          .withStorageResolution(
            if (highResolution) highResolutionFrequency
            else standardResolutionFrequency
          )
          .withUnit(standardUnit)
      )

    }
  }

  private def stageMetricDatumWithRawSnapshot(
    metricConfigured: Boolean,
    metricName: String,
    snapshot: Snapshot,
    standardUnit: StandardUnit,
    metricData: List[MetricDatum]
  ): Unit = {
    if (metricConfigured) {
      val dimensionedName: DimensionedName = DimensionedName.decode(metricName)
      val total: Double = snapshot.getValues.toStream.sum

      val statisticSet: StatisticSet = new StatisticSet()
        .withSum(total)
        .withSampleCount(snapshot.size.toDouble)
        .withMinimum(snapshot.getMin.toDouble)
        .withMaximum(snapshot.getMax.toDouble)

      val dimensions: Set[Dimension] = new LinkedHashSet[Dimension](builder.globalDimensions)

      dimensions.add(
        new Dimension()
          .withName(dimensionNameType)
          .withValue(dimensionSnapshotSummary)
      )

      dimensions.addAll(dimensionedName.getDimensions().asJavaCollection)

      metricData.add(
        new MetricDatum()
          .withTimestamp(new Date(builder.clock.getTime))
          .withMetricName(dimensionedName.name)
          .withDimensions(dimensions)
          .withStatisticValues(statisticSet)
          .withStorageResolution(if (highResolution) highResolutionFrequency else standardResolutionFrequency)
          .withUnit(standardUnit)
      )
    }
  }

  private def cleanMetricValue(metricValue: Double): Double = {
    val absoluteValue: Double = Math.abs(metricValue)
    if (absoluteValue < smallestSendableValue) {
// Allow 0 through untouched, everything else gets rounded to smallestSendableValue
      if (absoluteValue > 0) {
        if (metricValue < 0) {
          -smallestSendableValue
        } else {
          smallestSendableValue
        }
      }
    } else if (absoluteValue > largestSendableValue) {
      if (metricValue < 0) {
        -largestSendableValue
      } else {
        largestSendableValue
      }
    }
    metricValue
  }

}

object CloudWatchReporter {

  private val logger: Logger = LoggerFactory.getLogger(classOf[CloudWatchReporter])

  // Visible for testing
  val dimensionNameType: String = "Type"

  // Visible for testing
  val dimensionGauge: String = "gauge"

  // Visible for testing
  val dimensionCount: String = "count"

  // Visible for testing
  val dimensionSnapshotSummary: String = "snapshot-summary"

  // Visible for testing
  val dimensionSnapshotMean: String = "snapshot-mean"

  // Visible for testing
  val dimensionSnapshotStdDev: String = "snapshot-std-dev"

  /**
   * PutMetricData function accepts an optional StorageResolution parameter.
   * 1 = publish high-resolution metrics, 60 = publish at standard 1-minute resolution.
   */
  private val highResolutionFrequency: Int = 1

  private val standardResolutionFrequency: Int = 60

  /**
   * Amazon CloudWatch rejects values that are either too small or too large.
   * Values must be in the range of 8.515920e-109 to 1.174271e+108 (Base 10) or 2e-360 to 2e360 (Base 2).
   * <p>
   * In addition, special values (e.g., NaN, +Infinity, -Infinity) are not supported.
   */
  private val smallestSendableValue: Double = 8.515920e-109

  private val largestSendableValue: Double = 1.174271e+108

  /**
   * Each CloudWatch API request may contain at maximum 20 datums
   */ 
  private val maximumDatumsPerRequest: Int = 20

  /**
   * Creates a new {@link Builder} that sends values from the given {@link MetricRegistry} to the given namespace
   * using the given CloudWatch client.
   *
   * @param metricRegistry {@link MetricRegistry} instance
   * @param client         {@link AmazonCloudWatchAsync} instance
   * @param namespace      the namespace. Must be non-null and not empty.
   * @return {@link Builder} instance
   */
  def forRegistry(
    metricRegistry: MetricRegistry,
    client: AmazonCloudWatchAsync,
    namespace: String
  ): Builder =
    new Builder(metricRegistry, client, namespace)

  class Builder(
    val metricRegistry: MetricRegistry,
    val cloudWatchAsyncClient: AmazonCloudWatchAsync,
    val namespace: String
  ) {

    var percentiles: Array[Percentile] = Array(P75, P95, P999)
    var oneMinuteMeanRate: Boolean = _
    var fiveMinuteMeanRate: Boolean = _
    var fifteenMinuteMeanRate: Boolean = _
    var meanRate: Boolean = _
    var arithmeticMean: Boolean = _
    var stdDev: Boolean = _
    var dryRun: Boolean = _
    var zeroValuesSubmission: Boolean = _
    var statisticSet: Boolean = _
    var jvmMetrics: Boolean = _
    var reportRawCountValue: Boolean = _
    var metricFilter: MetricFilter = MetricFilter.ALL
    var rateUnit: TimeUnit = TimeUnit.SECONDS
    var durationUnit: TimeUnit = TimeUnit.MILLISECONDS
    var cwMeterUnit: Optional[StandardUnit] = Optional.empty()
    var cwRateUnit: StandardUnit = toStandardUnit(rateUnit)
    var cwDurationUnit: StandardUnit = toStandardUnit(durationUnit)
    var globalDimensions: Set[Dimension] = new LinkedHashSet()
    val clock: Clock = Clock.defaultClock()
    var highResolution: Boolean = _

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    def convertRatesTo(rateUnit: TimeUnit): Builder = {
      this.rateUnit = rateUnit
      this
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    def convertDurationsTo(durationUnit: TimeUnit): Builder = {
      this.durationUnit = durationUnit
      this
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param metricFilter a {@link MetricFilter}
     * @return {@code this}
     */
    def filter(metricFilter: MetricFilter): Builder = {
      this.metricFilter = metricFilter
      this
    }

    /**
     * If the one minute rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
     * <p>
     * The rate values are converted before reporting based on the rate unit set
     *
     * @return {@code this}
     * @see ScheduledReporter#convertRate(double)
     * @see Meter#getOneMinuteRate()
     * @see Timer#getOneMinuteRate()
     */
    def withOneMinuteMeanRate(): Builder = {
      oneMinuteMeanRate = true
      this
    }

    /**
     * If the five minute rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
     * <p>
     * The rate values are converted before reporting based on the rate unit set
     *
     * @return {@code this}
     * @see ScheduledReporter#convertRate(double)
     * @see Meter#getFiveMinuteRate()
     * @see Timer#getFiveMinuteRate()
     */
    def withFiveMinuteMeanRate(): Builder = {
      fiveMinuteMeanRate = true
      this
    }

    /**
     * If the fifteen minute rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
     * <p>
     * The rate values are converted before reporting based on the rate unit set
     *
     * @return {@code this}
     * @see ScheduledReporter#convertRate(double)
     * @see Meter#getFifteenMinuteRate()
     * @see Timer#getFifteenMinuteRate()
     */
    def withFifteenMinuteMeanRate(): Builder = {
      fifteenMinuteMeanRate = true
      this
    }

    /**
     * If the mean rate should be sent for {@link Meter} and {@link Timer}. {@code false} by default.
     * <p>
     * The rate values are converted before reporting based on the rate unit set
     *
     * @return {@code this}
     * @see ScheduledReporter#convertRate(double)
     * @see Meter#getMeanRate()
     * @see Timer#getMeanRate()
     */
    def withMeanRate(): Builder = {
      meanRate = true
      this
    }

    /**
     * If the arithmetic mean of {@link Snapshot} values in {@link Histogram} and {@link Timer} should be sent.
     * {@code false} by default.
     * <p>
     * The {@link Timer#getSnapshot()} values are converted before reporting based on the duration unit set
     * The {@link Histogram#getSnapshot()} values are reported as is
     *
     * @return {@code this}
     * @see ScheduledReporter#convertDuration(double)
     * @see Snapshot#getMean()
     */
    def withArithmeticMean(): Builder = {
      arithmeticMean = true
      this
    }

    /**
     * If the standard deviation of {@link Snapshot} values in {@link Histogram} and {@link Timer} should be sent.
     * {@code false} by default.
     * <p>
     * The {@link Timer#getSnapshot()} values are converted before reporting based on the duration unit set
     * The {@link Histogram#getSnapshot()} values are reported as is
     *
     * @return {@code this}
     * @see ScheduledReporter#convertDuration(double)
     * @see Snapshot#getStdDev()
     */
    def withStdDev(): Builder = {
      stdDev = true
      this
    }

    /**
     * If lifetime {@link Snapshot} summary of {@link Histogram} and {@link Timer} should be translated
     * to {@link StatisticSet} in the most direct way possible and reported. {@code false} by default.
     * <p>
     * The {@link Snapshot} duration values are converted before reporting based on the duration unit set
     *
     * @return {@code this}
     * @see ScheduledReporter#convertDuration(double)
     */
    def withStatisticSet(): Builder = {
      statisticSet = true
      this
    }

    /**
     * If JVM statistic should be reported. Supported metrics include:
     * <p>
     * - Run count and elapsed times for all supported garbage collectors
     * - Memory usage for all memory pools, including off-heap memory
     * - Breakdown of thread states, including deadlocks
     * - File descriptor usage
     * - Buffer pool sizes and utilization (Java 7 only)
     * <p>
     * {@code false} by default.
     *
     * @return {@code this}
     */
    def withJvmMetrics(): Builder = {
      jvmMetrics = true
      this
    }

    /**
     * Does not actually POST to CloudWatch, logs the {@link PutMetricDataRequest putMetricDataRequest} instead.
     * {@code false} by default.
     *
     * @return {@code this}
     */
    def withDryRun(): Builder = {
      dryRun = true
      this
    }

    /**
     * POSTs to CloudWatch all values. Otherwise, the reporter does not POST values which are zero in order to save
     * costs. Also, some users have been experiencing {@link InvalidParameterValueException} when submitting zero
     * values. Please refer to:
     * https://github.com/azagniotov/codahale-aggregated-metrics-cloudwatch-reporter/issues/4
     * <p>
     * {@code false} by default.
     *
     * @return {@code this}
     */
    def withZeroValuesSubmission(): Builder = {
      zeroValuesSubmission = true
      this
    }

    /**
     * Will report the raw value of count metrics instead of reporting only the count difference since the last
     * report
     * {@code false} by default.
     *
     * @return {@code this}
     */
    def withReportRawCountValue(): Builder = {
      reportRawCountValue = true
      this
    }

    /**
     * The {@link Histogram} and {@link Timer} percentiles to send. If <code>0.5</code> is included, it'll be
     * reported as <code>median</code>.This defaults to <code>0.75, 0.95 and 0.999</code>.
     * <p>
     * The {@link Timer#getSnapshot()} percentile values are converted before reporting based on the duration unit
     * The {@link Histogram#getSnapshot()} percentile values are reported as is
     *
     * @param percentiles the percentiles to send. Replaces the default percentiles.
     * @return {@code this}
     */
    def withPercentiles(percentiles: Percentile*): Builder = {
      this.percentiles = percentiles.toArray
      this
    }

    /**
     * Global {@link Set} of {@link Dimension} to send with each {@link MetricDatum}. A dimension is a name/value
     * pair that helps you to uniquely identify a metric. Every metric has specific characteristics that describe
     * it, and you can think of dimensions as categories for those characteristics.
     * <p>
     * Whenever you add a unique name/value pair to one of your metrics, you are creating a new metric.
     * Defaults to {@code empty} {@link Set}.
     *
     * @param dimensions arguments in a form of {@code name=value}. The number of arguments is variable and may be
     *                   zero. The maximum number of arguments is limited by the maximum dimension of a Java array
     *                   as defined by the Java Virtual Machine Specification. Each {@code name=value} string
     *                   will be converted to an instance of {@link Dimension}
     * @return {@code this}
     */
    def withGlobalDimensions(dimensions: String*): Builder = {

      for (pair <- dimensions) {

        val splitted: List[String] = pair
          .split("=")
          .toStream
          .map(_.trim)
          .asJava

        this.globalDimensions.add(
          new Dimension()
            .withName(splitted.get(0))
            .withValue(splitted.get(1))
        )
      }
      this
    }

    def withHighResolution(): Builder = {
      this.highResolution = true
      this
    }

    /**
     * Send Meters in other Unit than the DurationUnit. Useful if the metered metric does not contain time units
     * @param reportUnit the Unit which is set as metadata on meter reports.
     * @return {@code this}
     */
    def withMeterUnitSentToCW(reportUnit: StandardUnit): Builder = {
      this.cwMeterUnit = Optional.of(reportUnit)
      this
    }

    def build(): CloudWatchReporter = {
      cwRateUnit = cwMeterUnit.orElse(toStandardUnit(rateUnit))
      cwDurationUnit = toStandardUnit(durationUnit)
      new CloudWatchReporter(this)
    }

    private def toStandardUnit(timeUnit: TimeUnit): StandardUnit =
      timeUnit match {
        case TimeUnit.SECONDS => StandardUnit.Seconds
        case TimeUnit.MILLISECONDS => StandardUnit.Milliseconds
        case TimeUnit.MICROSECONDS => StandardUnit.Microseconds
        case _ =>
          throw new IllegalArgumentException("Unsupported TimeUnit: " + timeUnit)

      }

  }

}
