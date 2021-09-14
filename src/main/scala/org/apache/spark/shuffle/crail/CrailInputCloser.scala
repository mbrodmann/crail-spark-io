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

import org.apache.spark.TaskContext
import org.apache.spark.storage.BlockManager
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.serializer.CrailDeserializationStream

/**
 * Created by stu on 24.09.16.
 */
class CrailInputCloser[K, C](deserializationStream: CrailDeserializationStream, baseIter: Iterator[Product2[K, C]],
   taskContext: TaskContext, blockManager: BlockManager, shuffleId: Int, mapId: Long, mapIndex: Int, reduceId: Int) extends Iterator[Product2[K, C]]{


  override def hasNext: Boolean = {
    val ret = baseIter.hasNext
    if(!ret) {
      deserializationStream.close()
    }
    ret
  }

  override def next(): Product2[K, C] = {
    try {
      baseIter.next()
    } catch {
      case e: Throwable => {
        println("Caught exception in CrailInputCloser.next() and marked as failed")
        val ffe = new FetchFailedException(blockManager.blockManagerId, shuffleId, 0, 0, 0, cause=e)
        taskContext.setFetchFailed(ffe)
        //taskContext.markTaskFailed(e)
      }
      return null
    }
  }


//  override def hasNext: Boolean = {
//    baseIter.hasNext
//  }
//
//  override def next(): Product2[K, C] = {
//    val product = baseIter.next()
//    if (!hasNext) {
//      deserializationStream.close()
//    }
//    product
//  }
}
