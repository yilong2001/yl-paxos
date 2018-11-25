package com.example.paxos.util

import java.util.concurrent.ScheduledExecutorService


/**
  * Created by yilong on 2018/11/9.
  */
object PaxosUtils {
  def getString(value : Array[Byte]) : String = {
    if (value == null) "null"
    else new String(value, "utf-8")
  }

  def consensusThreshold(instanceNum : Int) : Int = {
    instanceNum % 2 == 0 match {
      case true => instanceNum / 2 + 1
      case _ => (instanceNum + 1)/2
    }
  }

  def schedulePeriodly[T](service : ScheduledExecutorService,
                       call : java.util.concurrent.Callable[T],
                       intervalFunc : ()=>Int) : java.util.concurrent.ScheduledFuture[_] = {
    service.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        call.call()
      }
    },0, intervalFunc(), java.util.concurrent.TimeUnit.MILLISECONDS)
  }

  def scheduleRecursive(service : ScheduledExecutorService,
                        call : java.util.concurrent.Callable[Boolean],
                        intervalFunc : ()=>Int) : java.util.concurrent.ScheduledFuture[_] = {
    val timerfuture = service.schedule(new Runnable {
      override def run(): Unit = {
        if (!call.call()) {
          scheduleRecursive(service, call, intervalFunc)
        }
      }
    }, intervalFunc(), java.util.concurrent.TimeUnit.MILLISECONDS)

    timerfuture
  }
}
