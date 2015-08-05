package me.arturopala.data

import org.scalatest.{ WordSpecLike, Matchers }
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.time.Instant

class MeetingDetectorSpec extends WordSpecLike with Matchers {

  import MeetingDetector._

  "A Data" should {
    "read reduced.csv" ignore {
      val url = this.getClass.getResource("/movements.csv")
      val uid1 = "625f6cb6"
      val uid2 = "3007face"
      val data = Data.read(url, uid1, uid2)
      data should not be empty
    }
  }

  "A SlidingTimeWindowIterator" should {
    "iterate over sequence of evenly distributed observations" in {
      val now = Instant.now()
      val size = 60
      val data = for (s <- 0 until size) yield Seq(now.plusSeconds(s).toString, s.toString, s.toString, "0", { if (s % 2 == 0) "a" else "b" })
      val s: Observations = data map (seq => Observation(seq))
      val n = 5
      val timeWindow = Duration.of(n, ChronoUnit.SECONDS)
      var c = 0
      for (tw <- s.sliding(timeWindow)) {
        tw._1.size should be((n + 1) / 2)
        tw._2.size should be((n + 1) / 2)
        c = c + 1
      }
      c should be(size - n)
    }
    "iterate over sequence of non-continous observations" in {
      val now = Instant.now()
      val data1 = for (s <- 0 until 10) yield Seq(now.plusSeconds(s).toString, s.toString, s.toString, "0", { if (s % 2 == 0) "a" else "b" })
      val data2 = for (s <- 20 until 30) yield Seq(now.plusSeconds(s).toString, s.toString, s.toString, "0", { if (s % 2 == 0) "a" else "b" })
      val data3 = for (s <- 40 until 50) yield Seq(now.plusSeconds(s).toString, s.toString, s.toString, "0", "a")
      val data = data1 ++ data2 ++ data3
      val s: Observations = data map (seq => Observation(seq))
      val n = 3
      val timeWindow = Duration.of(n, ChronoUnit.SECONDS)
      var c = 0
      for (tw <- s.sliding(timeWindow)) {
        tw._1.size should be <= ((n + 1) / 2)
        tw._2.size should be <= ((n + 1) / 2)
        c = c + 1
      }
      c should be(18)
    }
  }

  "A Observer" should {
    "find meetings" in {
      val now = Instant.now()
      val size = 60
      val s1 = (for (s <- 0 until size) yield Seq(now.plusSeconds(s).toString, (s + 0.1).toString, (s - 0.1).toString, "0", "a")) map Observation.apply
      val s2 = (for (s <- 0 until size) yield Seq(now.plusSeconds(s).plusMillis(350).toString, s.toString, (30 - s).toString, "0", "b")) map Observation.apply
      val s = (s1 ++ s2).sorted
      val result1 = Observer.findMeetings(s, Duration.of(5, ChronoUnit.SECONDS), 0.15)
      result1 should have size 1
      val result2 = Observer.findMeetings(s, Duration.of(20, ChronoUnit.SECONDS), 0.15)
      result2 should have size 1
      val result3 = Observer.findMeetings(s, Duration.of(10, ChronoUnit.SECONDS), 1.5)
      result3 should have size 4
    }
    "not find meetings when distance too long" in {
      val now = Instant.now()
      val size = 60
      val s1 = (for (s <- 0 until size) yield Seq(now.plusSeconds(s).toString, s.toString, s.toString, "0", "a")) map Observation.apply
      val s2 = (for (s <- 0 until size) yield Seq(now.plusSeconds(s).plusMillis(10).toString, s.toString, (s + 1.001).toString, "0", "b")) map Observation.apply
      val s = (s1 ++ s2).sorted
      val result1 = Observer.findMeetings(s, Duration.of(5, ChronoUnit.SECONDS), 1)
      result1 should have size 0
    }
    "not find meetings when distance time window too short" in {
      val now = Instant.now()
      val size = 60
      val s1 = (for (s <- 0 until size) yield Seq(now.plusSeconds(s).toString, (s + 0.1).toString, (s - 0.1).toString, "0", "a")) map Observation.apply
      val s2 = (for (s <- 0 until size) yield Seq(now.plusSeconds(s).plusMillis(350).toString, s.toString, (30 - s).toString, "0", "b")) map Observation.apply
      val s = (s1 ++ s2).sorted
      val result1 = Observer.findMeetings(s, Duration.of(300, ChronoUnit.MILLIS), 10)
      result1 should have size 0
    }
  }

  "A MeetingDetector" should {
    "find meetings" ignore {
      val uid1 = "625f6cb6"
      val uid2 = "3007face"
      val timeWindow = 60L
      val maxProximity = 1d
      val listSize = 10
      println(s"Finding meetings between $uid1 and $uid2 within $timeWindow seconds at max. proximity $maxProximity ...")

      val data = Data.read(this.getClass.getResource("/movements.csv"), uid1, uid2)
      val results = Util.log("whole search") {
        data.values flatMap (Observer.findMeetings(_, Duration.of(timeWindow, ChronoUnit.SECONDS), maxProximity))
      }
      val normalized = results map Data.normalizeEachOfPairWithin(100 * timeWindow, 100 * maxProximity)_ toSet

      if (results.size > 0) {
        println(s"Found ${results.size} meeting event(s) (${normalized.size} substantially different) of $uid1 and $uid2:")
        results.take(listSize) foreach {
          case (a, b) => println(s"${a.timestamp} @ ${a.floor} floor, close to (${a.x},${a.y})")
        }
        if (results.size > listSize) println(s" and ${results.size - listSize} others ...")
      } else {
        println(s"None meeting found within given time window and proximity.")
      }
    }
  }

}