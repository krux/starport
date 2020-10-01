package com.krux.starport.metric

import java.util.regex.{Matcher, Pattern}

import com.amazonaws.services.cloudwatch.model.Dimension

class DimensionedName private (name: String, dimensions: Map[String, Dimension]) {

  def getName: String = name

  def getDimensions: Set[Dimension] = dimensions.values.toSet

}

object DimensionedName {

  case class Builder(
    private val name: String,
    private val dimensions: Map[String, Dimension] = Map().empty
  ) {

    def withName(inputName: String): Builder = copy(name = inputName)

    def withDimension(name: String, value: String): Builder =
      copy(dimensions = Map(name -> new Dimension().withName(name).withValue(value)))

    def build(): DimensionedName = new DimensionedName(this.name, this.dimensions)

  }

  private val dimensionPattern: Pattern =
    Pattern.compile("([\\w.-]+)\\[([\\w\\W]+)]")

  def decode(encodedDimensionedName: String): DimensionedName = {
    val matcher: Matcher = dimensionPattern.matcher(encodedDimensionedName)
    if (matcher.find() && matcher.groupCount() == 2) {
      val builder: Builder = Builder(matcher.group(1).trim)
      for (t <- matcher.group(2).split(",")) {
        val keyAndValue: Array[String] = t.split(":")
        builder.withDimension(keyAndValue(0).trim(), keyAndValue(1).trim())
      }
      builder.build()
    } else {
      Builder(encodedDimensionedName).build()
    }
  }

}
