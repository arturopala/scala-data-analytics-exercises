package me.arturopala.data

/** Generic activity analyzer interface */
trait ActivityAnalyzer[Activity] {

  type Mapper[+Key] = Function[Activity, Key]
  type Filter = Function[Activity, Boolean]

  /** Calculates number of unique entities in the buckets of provided key */
  def calculateNumberOfUniqueEnitiesPer[Key, Count: Numeric](
    filter: Filter,
    keyMapper: Mapper[Key],
    identifier: Mapper[Any]): Map[Key, Count]

  /**
   * Calculates number of unique entities in the buckets of provided key.
   * Uses provided cache of unique identities per key to avoid recalculating.
   */
  def calculateNumberOfUniqueEnitiesPerWithCache[Key, Count: Numeric](
    filter: Filter,
    keyMapper: Mapper[Key],
    identifier: Mapper[Any])(cache: Map[Key, Set[Any]]): Map[Key, Count]

  /** Calculates distribution of pause between activities */
  def calculateDistributionOfPauseBetweenActivitesOfSameEntities[Pause: Ordering, Count: Numeric](
    filter: Filter,
    identifier: Mapper[Any],
    pauseCalculator: (Activity, Activity) => Pause): Seq[(Pause, Count)]

}

/* Simple activity analyzer implementation using Scala collection framework functions to process data */
trait SimpleActivityAnalyzer[Activity] extends ActivityAnalyzer[Activity] {

  import IteratorOps._

  /** abstract stream of pre-sorted activities to analyze */
  def activities: Iterable[Activity]

  override def calculateNumberOfUniqueEnitiesPer[Key, Count: Numeric](
    filter: Filter,
    mapper: Mapper[Key],
    identifier: Mapper[Any]): Map[Key, Count] = {

    val zero: Count = implicitly[Numeric[Count]].zero
    val one: Count = implicitly[Numeric[Count]].one

    val reducer = (sum: Count, activity: Activity) => implicitly[Numeric[Count]].plus(sum, one)

    Util.log("calculateNumberOfUniqueEnitiesPerKey")(
      activities
        .iterator
        .filter(filter)
        .reduceDistinctByKey(zero)(mapper, reducer, identifier)
    )
  }

  override def calculateNumberOfUniqueEnitiesPerWithCache[Key, Count: Numeric](
    filter: Filter,
    mapper: Mapper[Key],
    identifier: Mapper[Any])(cache: Map[Key, Set[Any]]): Map[Key, Count] = {

    val zero: Count = implicitly[Numeric[Count]].zero
    val one: Count = implicitly[Numeric[Count]].one

    val reducer = (sum: Count, activity: Activity) => implicitly[Numeric[Count]].plus(sum, one)

    Util.log("calculateNumberOfUniqueEnitiesPerWithCache")(
      activities
        .iterator
        .filter(filter)
        .reduceDistinctByKeyUsingCache(zero)(mapper, reducer, identifier)(cache)
    )
  }

  override def calculateDistributionOfPauseBetweenActivitesOfSameEntities[Pause: Ordering, Count: Numeric](
    filter: Filter,
    identifier: Mapper[Any],
    pauseCalculator: (Activity, Activity) => Pause): Seq[(Pause, Count)] = {

    val zero: Count = implicitly[Numeric[Count]].zero
    val one: Count = implicitly[Numeric[Count]].one

    val reducer = (sum: Count, pause: Pause) => implicitly[Numeric[Count]].plus(sum, one)

    Util.log("calculateDistributionOfPauseBetweenActivitesOfSameEntities")(
      activities
        .iterator
        .filter(filter)
        .slidePairsByKeyAndThenReduceByResultKey(zero)(identifier, pauseCalculator, reducer)
        .toSeq
        .sortBy { case (pause, _) => pause }
    )
  }

}

