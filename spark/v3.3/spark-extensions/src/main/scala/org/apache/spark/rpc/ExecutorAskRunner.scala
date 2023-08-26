/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.rpc

import java.util.concurrent.TimeUnit
import org.apache.iceberg.relocated.com.google.common.cache.CacheBuilder
import org.apache.iceberg.relocated.com.google.common.cache.CacheLoader
import org.apache.iceberg.relocated.com.google.common.cache.LoadingCache
import org.apache.spark.SparkConf
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class ExecutorAskRunner (override val rpcEnv: RpcEnv, val conf: SparkConf)
  extends RpcEndpoint with Logging {

  // Signer sts credential now expires in 12 hours, our cache expiration is set a bit earlier
  // to offset potential network delay in receiving the initial response.
  // https://go.netflix.com/CjkbwO
  private val cacheExpireInMinutes = 715

  private lazy val executorReplyCache: LoadingCache[ExecutorAsk, AnyRef] = {
    CacheBuilder
      .newBuilder()
      .expireAfterWrite(cacheExpireInMinutes, TimeUnit.MINUTES)
      .build(new CacheLoader[ExecutorAsk, AnyRef] {
        override def load(executorAsk: ExecutorAsk): AnyRef = {
          ThreadUtils.awaitResult(Future {
            val res = executorAsk.run
            logInfo("successfully loaded executor ask for key: " + executorAsk.cacheKey)
            res
          }(ThreadUtils.sameThread), 10 seconds)
        }
      })
  }

  /**
   * Process messages from `RpcEndpointRef.ask`. If receiving a unmatched message,
   * `SparkException` will be thrown and sent to `onError`.
   */
  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case ask: ExecutorAsk =>
      val reply = executorReplyCache.get(ask)
      context.reply(reply)
    case a =>
      context.sendFailure(new SparkException(self + " won't reply anything" + a.toString))
  }
}

abstract class ExecutorAsk extends Serializable {
  def run: AnyRef
  def cacheKey: String

  override def equals(obj: Any): Boolean = {
    obj match {
      case obj: ExecutorAsk =>
        obj.cacheKey == cacheKey
      case _ => false
    }
  }

  override def hashCode(): Int = cacheKey.hashCode
}
