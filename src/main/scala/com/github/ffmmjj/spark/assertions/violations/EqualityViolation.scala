package com.github.ffmmjj.spark.assertions.violations

trait EqualityViolation {
  def notFound: Boolean
  def toString: String
}
