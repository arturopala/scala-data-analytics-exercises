package me.arturopala.data

import java.net.URL
import scala.annotation.switch

/** Mobile phone activity analytics factory */
object SampleMobilePhoneActivity extends MobilePhoneActivityApi {

  /** Creates activity analyzer instance for the provided stream of mobile phone activity and subscriber dictionary */
  def apply(activities: Iterable[MobilePhoneActivity], subscribers: Uid => Subscriber): MobilePhoneActivityAnalyzer = new MobilePhoneActivityAnalyzerImpl(activities, subscribers)

  def readMobilePhoneActivitiesFromUrl(url: URL): Iterable[MobilePhoneActivity] = {
    new Iterable[MobilePhoneActivity] {
      override def iterator: Iterator[MobilePhoneActivity] = scala.io.Source
        .fromURL(url)
        .getLines()
        .drop(1)
        .flatMap(line => {
          if (!line.isEmpty) {
            try {
              val Array(uid, cid, lac, timestamp, eventCode) = line.split(",")
              Some(MobilePhoneData(uid, cid, lac, timestamp.toLong, Events(eventCode)))
            } catch {
              case e: Exception => {
                println(s"Could not parse line as MobilePhoneActivity because of ${e.getClass.getName} ${e.getMessage}")
                None
              }
            }
          } else None
        })
    }
  }

  def readSubscribersMapFromUrl(url: URL): Map[String, Subscriber] = {
    val stream = scala.io.Source
      .fromURL(url)
      .getLines()
      .drop(1)
      .flatMap(line => {
        if (!line.isEmpty) {
          try {
            val Array(uid, gender, age) = line.split(",")
            Some((uid, SubscriberData(uid, age.toInt, gender)))
          } catch {
            case e: Exception => {
              println(s"Could not parse line as MobilePhoneActivity because of ${e.getClass.getName} ${e.getMessage}")
              None
            }
          }
        } else None
      })
      .toMap
    stream
  }

  sealed trait MobileNetworkEvent
  object Events {
    case object LocationUpdate extends MobileNetworkEvent
    case object PeriodicUpdate extends MobileNetworkEvent
    case object DevicePoweredUp extends MobileNetworkEvent
    case object DeviceDisconnected extends MobileNetworkEvent
    case object IncomingCallOrSms extends MobileNetworkEvent
    case object OutgoingCallOrSms extends MobileNetworkEvent
    case object Handover extends MobileNetworkEvent
    case object CallEnd extends MobileNetworkEvent
    case object Other extends MobileNetworkEvent

    def apply(code: String): MobileNetworkEvent =
      (code.trim().toInt: @switch) match {
        case 0 => LocationUpdate
        case 1 => PeriodicUpdate
        case 2 => DevicePoweredUp
        case 3 => DeviceDisconnected
        case 4 => IncomingCallOrSms
        case 10 => OutgoingCallOrSms
        case 6 => Handover
        case 9 => Handover
        case 22 => CallEnd
        case _ => Other
      }
  }

  type Uid = String
  type Age = Int
  type Location = String
  type Timestamp = Long
  type Event = MobileNetworkEvent

  case class MobilePhoneData(uid: String, cid: String, lac: String, timestamp: Long, event: MobileNetworkEvent) extends MobilePhoneActivity {
    require(uid != null && !uid.isEmpty, "uid cannot be null or empty")
    val location = lac + ":" + cid
  }

  case class SubscriberData(uid: String, age: Int, gender: String) extends Subscriber

  sealed class MobilePhoneActivityAnalyzerImpl(
    val activities: Iterable[Activity],
    val subscriberProvider: Uid => Subscriber)
      extends MobilePhoneActivityAnalyzer with SimpleActivityAnalyzer[Activity] {

    import IteratorOps._
    lazy val cache = activities.iterator.groupDistinctIdsByKey(SubscriberAge, SubscriberUid)
  }

}