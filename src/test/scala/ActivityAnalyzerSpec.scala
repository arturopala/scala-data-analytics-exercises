package me.arturopala.data

import org.scalatest.{ WordSpecLike, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.junit.JUnitRunner
import org.scalacheck._

class ActivityAnalyzerSpec extends WordSpecLike with Matchers with PropertyChecks {

  implicit override val generatorDrivenConfig = PropertyCheckConfig(minSize = 1, maxSize = 100, minSuccessful = 100, workers = 5)

  "A SimpleActivityAnalyzer" must {

    type Activity = (String, Int)

    import TestCaseGenerators._

    class TestActivityAnalyzer(val activities: Iterable[Activity]) extends SimpleActivityAnalyzer[Activity]

    "calculate number of unique enities in the buckets of provided key" in {
      forAll(numberOfUniqueEnitiesInBucketsTestCaseGenerator) { (tc: TestCase1) =>
        // given
        val analyzer = new TestActivityAnalyzer(tc.stream)
        // when
        val result = analyzer.calculateNumberOfUniqueEnitiesPer[Int, Long](
          pair => true, // accept all
          pair => pair._2, // age
          pair => pair._1 // id
        )
        // then
        result should contain theSameElementsAs tc.expectedResult
      }
    }

    "calculate distribution of pause between activites of same entities" in {
      forAll(distributionOfPauseTestCaseGenerator) { (tc: TestCase2) =>
        // given

        val analyzer = new TestActivityAnalyzer(tc.stream)
        // when
        val result = analyzer.calculateDistributionOfPauseBetweenActivitesOfSameEntities[Int, Long](
          pair => true, //accept all
          pair => pair._1, // id
          (pair1, pair2) => pair2._2 - pair1._2 // pause calculation
        )
        // then
        result should contain theSameElementsAs tc.expectedResult
      }
    }

  }
}