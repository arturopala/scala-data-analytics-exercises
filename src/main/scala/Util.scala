package me.arturopala.data

object Util {
  def log[T](description: String)(exec: => T): T = {
    val runtime = Runtime.getRuntime()
    val t0 = System.nanoTime()
    val m0 = runtime.freeMemory()
    val result = exec
    val t1 = System.nanoTime()
    val m1 = runtime.freeMemory()
    println(description + " exec time: " + (t1 - t0) / 1000000d + " millis, free memory: " + m0 + " -> " + m1 + " delta: " + (m1 - m0))
    result
  }
}