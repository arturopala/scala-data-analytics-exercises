package me.arturopala.data

/** Advanced set of operations on Iterators */
object IteratorOps {

  implicit class IteratorOps[T, K, C](iterator: Iterator[T]) {

    /**
     * Groups values by mapped key and then reduce each sequence separately
     * @param zero initial value of reducer
     * @param mapper function returning key of given item
     * @param reducer function reducing items of the same key
     * @return map of key to reduced value (possibly count)
     */
    def reduceByKey(zero: C)(mapper: T => K, reducer: (C, T) => C): Map[K, C] = {
      val result = collection.mutable.Map[K, C]()
      while (iterator.hasNext) {
        val elem: T = iterator.next
        val key: K = mapper(elem)
        val accum: C = result.getOrElse(key, zero)
        result(key) = reducer(accum, elem)
      }
      result.toMap
    }

    /**
     * Groups values by mapped key and then reduce each sequence separately using only distinct items
     * @param zero initial value of reducer
     * @param mapper function returning key of given item
     * @param reducer function reducing items of the same key
     * @param identifier function returning id of an item
     * @return map of key to reduced value (possibly count)
     */
    def reduceDistinctByKey(zero: C)(mapper: T => K, reducer: (C, T) => C, identifier: T => Any): Map[K, C] = {
      import collection.mutable.{ Set, Map }
      val result = Map[K, C]()
      var cache = Map[K, Set[Any]]()
      while (iterator.hasNext) {
        val elem: T = iterator.next
        val key: K = mapper(elem)
        val id: Any = identifier(elem)
        val accum: C = result.getOrElse(key, zero)
        val ids: Set[Any] = cache.getOrElse(key, Set[Any]())
        if (ids.add(id)) {
          cache(key) = ids
          result(key) = reducer(accum, elem)
        }
      }
      cache = null
      result.toMap
    }

    /**
     * Groups distinct values of ids by key
     * @param  mapper function returning key of given item
     * @param identifier function returning id of an item
     * @return map of key to set of distinct ids
     */
    def groupDistinctIdsByKey[Id](mapper: T => K, identifier: T => Id): Map[K, Set[Id]] = {
      import collection.mutable.{ Map }
      val cache = Map[K, Set[Id]]()
      while (iterator.hasNext) {
        val elem: T = iterator.next
        val key: K = mapper(elem)
        val id: Id = identifier(elem)
        val ids: Set[Id] = cache.getOrElse(key, Set[Id]())
        if (!ids.contains(id)) cache(key) = ids + id
      }
      cache.toMap
    }

    /**
     * Groups values by mapped key and then reduce each sequence separately using only distinct items.
     * Uses provided cached map of distinct identities to optimize footprint.
     * @param zero initial value of reducer
     * @param mapper function returning key of given item
     * @param reducer function reducing items of the same key
     * @param identifier function returning id of an item
     * @param cache map of all keys and corresponding item identities
     * @return map of key to reduced value (possibly count)
     */
    def reduceDistinctByKeyUsingCache(zero: C)(mapper: T => K, reducer: (C, T) => C, identifier: T => Any)(cache: Map[K, Set[Any]]): Map[K, C] = {
      import collection.mutable.{ Set, Map }
      val result = Map[K, C]()
      while (iterator.hasNext) {
        val elem: T = iterator.next
        val key: K = mapper(elem)
        val id: Any = identifier(elem)
        val accum: C = result.getOrElse(key, zero)
        val ids: Option[Any] = cache.get(key)
        if (ids.isDefined && !ids.contains(id)) {
          result(key) = reducer(accum, elem)
        }
      }
      result.toMap
    }

    /**
     * Groups values by mapped key and then slides over each sequence separately producing pairs of values
     * as an input to the calculator. Calculated results are then grouped by its value and reduced separately using
     * provided reducer.
     * @type P type of calculated value
     * @param mapper function returning key of given item
     * @param calculator function used to calculate resulting value from the given pair of items
     * @param reducer function reducing calculated values
     * @return map of calculated value to reduced value (possibly count)
     */
    def slidePairsByKeyAndThenReduceByResultKey[P](zero: C)(mapper: T => K, calculator: (T, T) => P, reducer: (C, P) => C): Map[P, C] = {
      import collection.mutable.{ Set, Map }
      val previousElementsMap = Map[K, T]()
      val resultsMap = Map[P, C]()
      while (iterator.hasNext) {
        val current: T = iterator.next
        val key: K = mapper(current)
        val previousElemOpt: Option[T] = previousElementsMap.get(key)
        for (previous <- previousElemOpt) {
          val value: P = calculator(previous, current)
          val accum: C = resultsMap.getOrElse(value, zero)
          resultsMap(value) = reducer(accum, value)
        }
        previousElementsMap(key) = current
      }
      resultsMap.toMap
    }

  }
}