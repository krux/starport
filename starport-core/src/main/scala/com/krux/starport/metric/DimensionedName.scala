package com.krux.starport.metric

import com.amazonaws.services.cloudwatch.model.Dimension

import java.util.regex.{Matcher, Pattern}

import scala.collection.mutable.{HashMap, Map}

case class DimensionedName(name: String, dimensions: Map[String, Dimension]) {

  def withDimension(name: String, value: String): DimensionedNameBuilder =
    DimensionedNameBuilder(this.name, new HashMap() ++ this.dimensions)
      .withDimension(name, value)

  def getDimensions(): Set[Dimension] = dimensions.values.toSet

}

object DimensionedName {

  private val dimensionPattern: Pattern =
    Pattern.compile("([\\w.-]+)\\[([\\w\\W]+)]")

  def decode(encodedDimensionedName: String): DimensionedName = {
    val matcher: Matcher = dimensionPattern.matcher(encodedDimensionedName)
    if (matcher.find() && matcher.groupCount() == 2) {
      val builder: DimensionedNameBuilder = new DimensionedNameBuilder(
        matcher.group(1).trim)
      for (t <- matcher.group(2).split(",")) {
        val keyAndValue: Array[String] = t.split(":")
        builder.withDimension(keyAndValue(0).trim(), keyAndValue(1).trim())
      }
      builder.build()
    } else {
      new DimensionedNameBuilder(encodedDimensionedName).build()
    }
  }

  def withName(name: String): DimensionedNameBuilder =
    new DimensionedNameBuilder(name)

}

