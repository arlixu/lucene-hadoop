package org.apache.spark.cache

import org.apache.spark.util.AccumulatorV2

import java.util.concurrent.{ConcurrentHashMap, CopyOnWriteArraySet}
import scala.collection.JavaConverters._
import scala.collection.mutable._

class LuceneCacheAccumulator extends AccumulatorV2[Map[String, Set[String]], Map[String, Set[String]]] {

  private val map: ConcurrentHashMap[String, CopyOnWriteArraySet[String]] = new ConcurrentHashMap()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Map[String, Set[String]], Map[String, Set[String]]] = {
    val copyAccumulator = new LuceneCacheAccumulator()
    val iterator = map.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      copyAccumulator.map.put(entry.getKey, new CopyOnWriteArraySet(entry.getValue))
    }
    copyAccumulator
  }

  override def reset(): Unit = map.clear()

  override def add(input: Map[String, Set[String]]): Unit = {
    input.foreach { case (key, values) =>
      val existingValues = map.computeIfAbsent(key, _ => new CopyOnWriteArraySet())
      existingValues.addAll(values.asJava)
    }
  }

  override def merge(other: AccumulatorV2[Map[String, Set[String]], Map[String, Set[String]]]): Unit = {
    other match {
      case otherAccumulator: LuceneCacheAccumulator =>
        otherAccumulator.map.forEach { (key, values) =>
          val existingValues = map.computeIfAbsent(key, _ => new CopyOnWriteArraySet())
          existingValues.addAll(values)
        }
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def value: Map[String, Set[String]] = {
    val result = Map[String, Set[String]]()
    val iterator = map.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      result.put(entry.getKey, entry.getValue.asScala)
    }
    result
  }
}
