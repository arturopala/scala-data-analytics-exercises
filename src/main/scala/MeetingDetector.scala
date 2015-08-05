package me.arturopala.data

import java.time.Instant
import java.time.Duration
import java.time.temporal.ChronoUnit
import java.net.URL

object MeetingDetector {

  type Key = (String, String)
  type Observations = Seq[Observation]

  case class Observation(timestamp: String, x: Double, y: Double, floor: String, uid: String) {
    lazy val dateTime = Instant.parse(timestamp)
  }

  object Observation {
    def apply(arr: Seq[String]): Observation =
      Observation(arr(0), java.lang.Double.valueOf(arr(1)), java.lang.Double.valueOf(arr(2)), arr(3), arr(4))
    def dateAndFloor = (o: Observation) => (o.timestamp.substring(0, 9), o.floor)
    def uid = (o: Observation) => o.uid
    def time = (o: Observation) => o.timestamp.substring(11, 23)
    def x = (o: Observation) => o.x
    def y = (o: Observation) => o.y
    def timestamp = (o: Observation) => o.timestamp

    implicit object ObservationOrdering extends Ordering[Observation] {
      def compare(o1: Observation, o2: Observation): Int = o1.dateTime.compareTo(o2.dateTime)
    }
  }

  object Data {

    def read(url: URL, uid1: String, uid2: String): Map[Key, Observations] = scala.io.Source
      .fromURL(url)
      .getLines()
      .toStream
      .drop(1)
      .filter(line => line.endsWith(uid1) || line.endsWith(uid2))
      .map { line => Observation(line.split(",")) }
      .groupBy(Observation.dateAndFloor)
      .mapValues(_
        .sortBy(Observation.timestamp)
      )

    def normalizeEachOfPairWithin(seconds: Long, proximity: Double)(p: (Observation, Observation)): (Observation, Observation) = {
      val n = normalizeWithin(seconds, proximity) _
      (n(p._1), n(p._2))
    }
    def normalizeWithin(seconds: Long, proximity: Double)(o: Observation): Observation =
      o.copy(
        timestamp = Instant.ofEpochSecond((o.dateTime.getEpochSecond / seconds) * seconds).toString,
        x = Math.rint(o.x / proximity) * proximity,
        y = Math.rint(o.y / proximity) * proximity
      )
  }

  object Observer {

    def findMeetings(stream: Observations, timeWindow: java.time.Duration, maxProximity: Double) = Util.log("find meetings") {
      stream.sliding(timeWindow) flatMap {
        case (s1, s2) =>
          findNeighbours(s1, s2, maxProximity) collect {
            case Some(x) => x
          }
      } toSet
    }

    def findNeighbours(s1: Observations, s2: Observations, maxProximity: Double) = {
      for (o1 <- s1; o2 <- s2) yield if (proximity(o1, o2) <= maxProximity) {
        if (o1.uid <= o2.uid) Some(o1, o2) else Some(o2, o1)
      } else None
    }

    def proximity(o1: Observation, o2: Observation) = java.lang.Math.hypot(o2.x - o1.x, o2.y - o1.y)

  }

  /**
   * Iterator sliding over a sequence of observations,
   * produces pairs of sequences containing observations within given time-window
   * partitioned by uid
   */
  final class SlidingTimeWindowIterator(stream: Observations, timeWindow: Duration) extends Iterator[(Observations, Observations)] {

    private val iterator = stream.iterator
    private var s1 = Vector[Observation](null)
    private var s2 = Vector.empty[Observation]
    private var delayed: Option[Observation] = None
    private var nextPairOpt = prepareNext()

    @scala.annotation.tailrec
    private def prepareNext(): Option[(Observations, Observations)] = {

      def chooseFirst(): Observation = {
        if (s1.isEmpty || (s2.nonEmpty && (s2.head.dateTime.compareTo(s1.head.dateTime) < 0))) {
          val temp = s1; s1 = s2; s2 = temp // swaping sequences if first empty or second has earlier head
        }
        if (s1.isEmpty) {
          if (delayed.isDefined) {
            s1 = s1 :+ delayed.get
            delayed = None
          } else {
            s1 = s1 :+ iterator.next
          }
        }
        s1.head
      }

      s1 = s1.drop(1)
      if (iterator.hasNext || delayed.isDefined) {
        val first = chooseFirst()
        var go = true
        while (go && (delayed.isDefined || iterator.hasNext)) {
          val current = delayed.getOrElse(iterator.next)
          go = if (Duration.between(first.dateTime, current.dateTime).compareTo(timeWindow) <= 0) {
            if (current.uid == first.uid) s1 = s1 :+ current else s2 = s2 :+ current
            delayed = None
            true
          } else {
            delayed = Some(current)
            false
          }
        }
        if (s1.nonEmpty && s2.nonEmpty) Some(s1, s2) else prepareNext()
      } else None
    }

    override def hasNext: Boolean = nextPairOpt.isDefined
    override def next: (Observations, Observations) = {
      val pair = nextPairOpt.get
      nextPairOpt = prepareNext()
      pair
    }
  }

  implicit class ObservationsOps(val stream: Observations) extends AnyVal {
    def sliding(timeWindow: Duration): Iterator[(Observations, Observations)] = new SlidingTimeWindowIterator(stream, timeWindow)
  }

}