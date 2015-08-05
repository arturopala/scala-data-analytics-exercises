package me.arturopala.data

import org.scalatest.{ WordSpecLike, Matchers }
import org.scalatest.prop.PropertyChecks
import org.scalatest.junit.JUnitRunner
import org.scalacheck._
import java.time.{ Instant, ZonedDateTime, ZoneOffset }
import java.time.temporal.ChronoField

class SampleMobilePhoneActivitySpec extends WordSpecLike with Matchers with PropertyChecks {

  "A SampleMobilePhoneActivity" must {

    "read sample mobile phone data and provide analyzer instance, then calculate basic characteristics" ignore {

      val dataUrl = this.getClass.getResource("/mobilephone_trace.csv")
      val crmUrl = this.getClass.getResource("/subscriber_data.csv")

      try { dataUrl.openStream().close() } catch { case ioe: Exception => throw new IllegalArgumentException("missing input file /src/main/resources/mobilephone_trace.csv") }
      try { crmUrl.openStream().close() } catch { case ioe: Exception => throw new IllegalArgumentException("missing input file /src/main/resources/subscriber_data.csv") }

      val activities = SampleMobilePhoneActivity.readMobilePhoneActivitiesFromUrl(dataUrl)

      val subscribers = SampleMobilePhoneActivity.readSubscribersMapFromUrl(crmUrl)
      val s = Util.log("Subscriber map read time")(subscribers.size)
      println(s"read $s subcribers data from input file")

      val analyzer = SampleMobilePhoneActivity(activities, subscribers)
      val zoneOffset = ZoneOffset.ofHours(2)

      println("I. Number of unique subscribers, in age buckets, for LocationUpdate event.")
      val perAge = analyzer.numberOfUniqueSubscribersPerAge[Int](SampleMobilePhoneActivity.Events.LocationUpdate)
      println(perAge)

      println("II. Number of unique subscribers during morning, afternoon, evening and night time.")
      val timeOfDay: Long => String = (timestamp: Long) => (ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), zoneOffset).get(ChronoField.CLOCK_HOUR_OF_DAY) match {
        case h if h < 6 => "night: 23h-6h"
        case h if h > 23 => "night: 23h-6h"
        case h if h < 12 => "morning: 6h-12h"
        case h if h < 17 => "afternoon: 12h-17h"
        case _ => "evening: 17h-23h"
      })
      val perAgeInTimeOfDay = analyzer.numberOfUniqueSubscribersPerTimeOfDay[String, Int](timeOfDay)
      println(perAgeInTimeOfDay)

      println("III. Number of unique subscribers, in age buckets, in the provided slice of time. Range of time is input for this method .")
      def isInTimeRange(fromMinute: Int, toMinute: Int) = (timestamp: Long) => {
        val minute = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), zoneOffset).get(ChronoField.MINUTE_OF_DAY)
        minute >= fromMinute && minute < toMinute
      }
      val perAgeInTimeRange = analyzer.numberOfUniqueSubscribersPerAgeInTimeRange[Int](isInTimeRange(311, 1254))
      println(perAgeInTimeRange)

      println("IV. Number of unique subscribers per location, time bucket (hourly­based): 20 first records")
      val hourOfDay = (timestamp: Long) => (ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp), zoneOffset).get(ChronoField.HOUR_OF_DAY))
      val perLocationAndHourOfDay = analyzer.numberOfUniqueSubscribersPerLocationAndHourOfDay[Int, Int](hourOfDay)
      println(perLocationAndHourOfDay.take(20))

      println("V. Distribution of pause between events from the same subscriber")
      val distributionOfPause = analyzer.distributionOfPauseBetweenEventsFromTheSameSubscriber[String, Int](
        (a0, a1) => (a1.timestamp - a0.timestamp) match {
          case p if p < 1 => "< 1 sec"
          case p if p < 10 => "1 sec - 10 sec"
          case p if p < 60 => "10 sec - 1 min"
          case p if p < 300 => "1 min - 5 min"
          case p if p < 3600 => "5 min - 1h"
          case p if p < 86400 => "1h - 24h"
          case _ => "> 24h"
        }
      )
      println(distributionOfPause)
    }

  }
}