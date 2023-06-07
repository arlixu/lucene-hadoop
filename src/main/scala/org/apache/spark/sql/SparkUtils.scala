package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.ExecutorCacheTaskLocation

object SparkUtils {
  def getExecutorLocations(sparkContext: SparkContext):Seq[String]={
    sparkContext.env.blockManager.master.getMemoryStatus.iterator.takeWhile(!_._1.executorId.equals("driver")
    ).map { case(blockManagerId, mem) =>
      ExecutorCacheTaskLocation(blockManagerId.host , blockManagerId.executorId).toString()
    }.toSeq
  }
}
