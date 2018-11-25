package com.example.paxos.protocol.exector

/**
  * Created by yilong on 2018/11/17.
  */
class RetryCount(baseCount : Long, maxCount : Long) {
  private var retry = baseCount * maxCount
  def isCountExpired() : Boolean = {
    (retry >= (baseCount+1)*maxCount)
  }

  def increaseCount() : Unit = {
    retry = retry + 1
  }

  def resetCount() : Unit = {
    retry = baseCount
  }

  def getCount() = retry
}
