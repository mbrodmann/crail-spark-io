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
import org.apache.spark.serializer.{CrailSerializer, SerializerManager}
import org.apache.spark.shuffle.{BaseShuffleHandle, CrailShuffleSorter, FetchFailedException, ShuffleReader}
import org.apache.spark.storage._


class CrailShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    crailSorter: CrailShuffleSorter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging
{
  require(endPartition == startPartition + 1, "Crail shuffle currently only supports fetching one partition")

  private val dep = handle.dependency
  private val serializerInstance = CrailDispatcher.get.getCrailSerializer().newCrailSerializer(dep.serializer)

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    
    var res: Iterator[Product2[K, C]] = null
    
    try {
      val multiStream = CrailDispatcher.get.getMultiStream(handle.shuffleId, startPartition)
      val deserializationStream = serializerInstance.deserializeCrailStream(multiStream)
      dep.keyOrdering match {
        case Some(keyOrd: Ordering[K]) =>
          res = new CrailInputCloser(deserializationStream, crailSorter.sort(context, keyOrd, dep.serializer, deserializationStream), context, blockManager, dep.shuffleId, 0, 0, 0)
        case None =>
          res = new CrailInputCloser(deserializationStream, deserializationStream.asKeyValueIterator.asInstanceOf[Iterator[Product2[K, C]]], context, blockManager, dep.shuffleId, 0, 0, 0)
      } 
    } catch {
      case e:Throwable => {
        println("Experimental: Error when reading shuffle data ...")
        val ffe = new FetchFailedException(blockManager.blockManagerId, dep.shuffleId, 0, 0, 0,cause=e)
        context.setFetchFailed(ffe)

        // create directories if gone missing
        val rootDir = CrailDispatcher.get.rootDir
        val shuffleDir = CrailDispatcher.get.shuffleDir
        val shuffleIdDir = shuffleDir+"/shuffle_"+dep.shuffleId

        try {
          CrailDispatcher.get.fs.create(rootDir, CrailNodeType.DIRECTORY, CrailDispatcher.get.shuffleStorageClass, CrailLocationClass.DEFAULT, true).get().syncDir()
        } catch {
          case e: Throwable =>
        }

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

