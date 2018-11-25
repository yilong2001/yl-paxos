package com.example.paxos.protocol.util

import com.example.srpc.nettyrpc.util.ThreadUtils

import scala.concurrent.{ExecutionContext, Future}

/**
  * Created by yilong on 2018/11/17.
  */
object ThreadServiceExecutor {
  def apply[T](body: =>T)(implicit executor: ExecutionContext): Future[T] = {
    val f = Future[T] {
      body
    }(executor)
    f
  }

  val execContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("ForPrepare", 32))

  val acceptContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("ForAccept", 32))

  val proposerTimerSchd = ThreadUtils.newDaemonSingleThreadScheduledExecutor("proposer timer")
  val syncTimerSchd = ThreadUtils.newDaemonSingleThreadScheduledExecutor("sync timer")

  def stop() : Unit = {
    execContext.shutdownNow()
    acceptContext.shutdownNow()
    proposerTimerSchd.shutdownNow()
    syncTimerSchd.shutdownNow()
  }
}
