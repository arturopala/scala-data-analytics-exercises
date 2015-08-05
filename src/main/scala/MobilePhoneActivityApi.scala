package me.arturopala.data

/** Generic defintion of mobile phone data analytics domain */
trait MobilePhoneActivityApi {

  type Activity = MobilePhoneActivity

  type Uid
  type Age
  type Location
  type Timestamp
  type Event

  trait Subscriber {
    def uid: Uid
    def age: Age
    def gender: String
  }

  trait MobilePhoneActivity {
    def uid: Uid
    def location: Location
    def timestamp: Timestamp
    def event: Event
  }

  /** Common inteface of mobile phone data analyzers */
  trait MobilePhoneActivityAnalyzer extends ActivityAnalyzer[Activity] {

    /** Function used to join mobile data with subscriber data */
    def subscriberProvider: Uid => Subscriber

    object AcceptAll extends Function[Activity, Boolean] {
      def apply(activity: Activity): Boolean = true
    }

    object SubscriberAge extends Function[Activity, Age] {
      def apply(activity: Activity): Age = subscriberProvider(activity.uid).age
    }

    object SubscriberUid extends Function[Activity, Uid] {
      def apply(activity: Activity): Uid = activity.uid
    }

    def numberOfUniqueSubscribersPerAge[Count: Numeric](event: Event): Map[Age, Count] = {
      val EventFilter: Filter = activity => activity.event == event
      calculateNumberOfUniqueEnitiesPer[Age, Count](EventFilter, SubscriberAge, SubscriberUid)
    }

    def numberOfUniqueSubscribersPerTimeOfDay[TimeOfDay, Count: Numeric](timeOfDay: Timestamp => TimeOfDay): Map[TimeOfDay, Count] = {
      val TimeOfDay: Mapper[TimeOfDay] = activity => timeOfDay(activity.timestamp)
      calculateNumberOfUniqueEnitiesPer[TimeOfDay, Count](AcceptAll, TimeOfDay, SubscriberUid)
    }

    def numberOfUniqueSubscribersPerAgeInTimeRange[Count: Numeric](isInTimeRange: Timestamp => Boolean): Map[Age, Count] = {
      val FittingTimeRange: Filter = activity => isInTimeRange(activity.timestamp)
      calculateNumberOfUniqueEnitiesPer[Age, Count](FittingTimeRange, SubscriberAge, SubscriberUid)
    }

    def numberOfUniqueSubscribersPerLocationAndHourOfDay[HourOfDay, Count: Numeric](hourOfDay: Timestamp => HourOfDay): Map[(Location, HourOfDay), Count] = {
      val LocationAndHourOfDay: Mapper[(Location, HourOfDay)] = activity => (activity.location, hourOfDay(activity.timestamp))
      calculateNumberOfUniqueEnitiesPer[(Location, HourOfDay), Count](AcceptAll, LocationAndHourOfDay, SubscriberUid)
    }

    def distributionOfPauseBetweenEventsFromTheSameSubscriber[Pause: Ordering, Count: Numeric](pauseBetween: (Activity, Activity) => Pause) = {
      calculateDistributionOfPauseBetweenActivitesOfSameEntities[Pause, Count](AcceptAll, SubscriberUid, pauseBetween)
    }

  }

}