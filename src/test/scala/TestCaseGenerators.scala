package me.arturopala.data

import org.scalacheck.Gen

object TestCaseGenerators {

  case class TestCase1(stream: Iterable[(String, Int)], expectedResult: Map[Int, Long])
  case class TestCase2(stream: Iterable[(String, Int)], expectedResult: Seq[(Int, Long)])

  val distributionOfPauseTestCaseGenerator: Gen[TestCase2] = Gen.resultOf((i: Int) =>
    TestCase2(
      stream = Seq("A" -> 1, "B" -> 1, "B" -> 1, "A" -> 2, "B" -> 2, "C" -> 1, "C" -> 2, "C" -> 2, "C" -> 4),
      expectedResult = Seq((0, 2), (1, 3), (2, 1))
    )
  )

  val numberOfUniqueEnitiesInBucketsTestCaseGenerator: Gen[TestCase1] = Gen.resultOf((i: Int) =>
    TestCase1(
      stream = Seq("A" -> 1, "B" -> 1, "B" -> 1, "A" -> 2, "B" -> 2, "C" -> 1, "C" -> 2, "C" -> 2, "C" -> 3),
      expectedResult = Map(1 -> 3, 2 -> 3, 3 -> 1)
    )
  )

}
