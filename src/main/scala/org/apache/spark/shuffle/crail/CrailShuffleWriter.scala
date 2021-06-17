/*
 * Spark-IO: Fast storage and network I/O for Spark
 *
 * Author: Patrick Stuedi <stu@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.shuffle.crail

import org.apache.crail.{CrailLocationClass, CrailNodeType, CrailStorageClass}
import org.apache.spark._
import org.apache.spark.common._
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.CrailSerializer
import org.apache.spark.shuffle._
import org.apache.spark.storage._


class CrailShuffleWriter[K, V](
                                shuffleBlockManager: CrailShuffleBlockResolver,
                                handle: BaseShuffleHandle[K, V, _],
                                mapId: Long,
                                context: TaskContext)
  extends ShuffleWriter[K, V] with Logging {

  private val dep = handle.dependency
  private val blockManager = SparkEnv.get.blockManager
  private var stopping = false
  private val writeMetrics = context.taskMetrics().shuffleWriteMetrics
  var serializerInstance =  CrailDispatcher.get.getCrailSerializer().newCrailSerializer(dep.serializer)
  var startTime : Double = System.nanoTime() / 1000
  private var shuffle : CrailShuffleWriterGroup = CrailDispatcher.get.getWriterGroup(dep.shuffleId, dep.partitioner.numPartitions, serializerInstance, writeMetrics)
  var initTime : Double = (System.nanoTime()/1000) - startTime
  var runTime : Double = 0
  var initRatio : Double = 0
  var overhead : Double = 0


  /** Write a bunch of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {

    try {
      val iter = if (dep.aggregator.isDefined) {
        if (dep.mapSideCombine) {
          dep.aggregator.get.combineValuesByKey(records, context)
        } else {
          records
        }
      } else {
        require(!dep.mapSideCombine, "Map-side combine without Aggregator specified!")
        records
      }

      for (elem <- iter) {
        val bucketId = dep.partitioner.getPartition(elem._1)
        shuffle.writers(bucketId).write(elem._1, elem._2)
      }
    } catch {
      case e : Throwable => {
        println("Experimental: Error when writing shuffle data ...")
        val ffe = new FetchFailedException(blockManager.blockManagerId, dep.shuffleId, mapId, 0, 0,cause=e)
        context.setFetchFailed(ffe)
        
        // cleanup
        //CrailDispatcher.get.releaseWriterGroup(dep.shuffleId, shuffle)
        //shuffle = CrailDispatcher.get.getWriterGroup(dep.shuffleId, dep.partitioner.numPartitions, serializerInstance, writeMetrics)
        
        // create directories if gone missing
        val shuffleDir = CrailDispatcher.get.shuffleDir
        val shuffleIdDir = shuffleDir+"/shuffle_"+dep.shuffleId
        
        try {
          CrailDispatcher.get.fs.create(shuffleDir, CrailNodeType.DIRECTORY, CrailDispatcher.get.shuffleStorageClass, CrailLocationClass.DEFAULT, true).get().syncDir()  
        } catch {
          case e: Throwable =>
        }

        try {
          CrailDispatcher.get.fs.create(shuffleIdDir, CrailNodeType.DIRECTORY, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true).get().syncDir()
        } catch {
          case e: Throwable =>
        }
        
        var i = 0
        while (i < dep.partitioner.numPartitions) {
          val subDir = shuffleIdDir + "/" + "part_" + i.toString
          try {
            CrailDispatcher.get.fs.create(subDir, CrailNodeType.MULTIFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true).get().syncDir()
          } catch {
            case e: Throwable =>
          }
          i+=1
        }
      }
    }


  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    
    var res: Option[MapStatus] = None
    
    try {
      if (stopping) {
        return None
      }
      stopping = true
      if (success) {
        shuffle.purge()
        val sizes: Array[Long] = shuffle.writers.map {writer => writer.length }
        CrailDispatcher.get.releaseWriterGroup(dep.shuffleId, shuffle)
        runTime = (System.nanoTime()/1000) - startTime
        initRatio = runTime/initTime
        overhead = 100/initRatio
        logInfo("shuffler writer: initTime " + initTime + ", runTime " + runTime + ", initRatio " + initRatio + ", overhead " + overhead)
        res =  Some(MapStatus(blockManager.shuffleServerId, sizes, context.taskAttemptId()))
      } else {
        res =  None
      }
    } catch {
      case e : Throwable => {
        println("Experimental: Error when stopping CrailShuffleWriter ...")
        val ffe = new FetchFailedException(blockManager.blockManagerId, dep.shuffleId, mapId, 0, 0,cause=e)
        context.setFetchFailed(ffe)

        // cleanup
        //CrailDispatcher.get.releaseWriterGroup(dep.shuffleId, shuffle)
        //shuffle = CrailDispatcher.get.getWriterGroup(dep.shuffleId, dep.partitioner.numPartitions, serializerInstance, writeMetrics)

        // create directories if gone missing
        val shuffleDir = CrailDispatcher.get.shuffleDir
        val shuffleIdDir = shuffleDir+"/shuffle_"+dep.shuffleId

        try {
          CrailDispatcher.get.fs.create(shuffleDir, CrailNodeType.DIRECTORY, CrailDispatcher.get.shuffleStorageClass, CrailLocationClass.DEFAULT, true).get().syncDir()
        } catch {
          case e: Throwable =>
        }

        try {
          CrailDispatcher.get.fs.create(shuffleIdDir, CrailNodeType.DIRECTORY, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true).get().syncDir()
        } catch {
          case e: Throwable =>
        }

        var i = 0
        while (i < dep.partitioner.numPartitions) {
          val subDir = shuffleIdDir + "/" + "part_" + i.toString
          try {
            CrailDispatcher.get.fs.create(subDir, CrailNodeType.MULTIFILE, CrailStorageClass.PARENT, CrailLocationClass.DEFAULT, true).get().syncDir()
          } catch {
            case e: Throwable =>
          }
          i+=1
        }
      }
    }
    
    res
  }
}
