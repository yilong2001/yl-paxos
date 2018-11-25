package com.example.paxos.protocol.exector

import java.util.concurrent.atomic.AtomicBoolean

import com.example.paxos.exception.PaxosException
import com.example.paxos.instance.PaxosInstance
import com.example.paxos.protocol.endpoint._
import com.example.paxos.protocol.role.Acceptor
import com.example.paxos.util.PaxosUtils
import org.apache.commons.logging.LogFactory

/**
  * Created by yilong on 2018/11/16.
  *
  * Acceptor run with message queue in AcceptorEndPoint
  */
class DefaultAcceptor(val instance : PaxosInstance) extends Acceptor {
  var curData : PaxosData = null
  private val log = LogFactory.getLog(classOf[DefaultAcceptor])
  val isWorking = new AtomicBoolean(false)

  override def onPrepare(data: PaxosData): Any = {
    log.info(s"onPrepare start, input data=${data.round},${data.posId},${PaxosUtils.getString(data.value)}")

    while(!isWorking.compareAndSet(false, true)) {
      //
    }

    val out = if (curData != null) {
      if (curData.round < data.round) {
        curData = PaxosData(data.round, data.posId, null)
        PaxosPrepareOk(curData)
      } else if (curData.round > data.round) {
        PaxosPrepareDeny()
      } else {
        if (curData.posId < data.posId) {
          curData = PaxosData(data.round, data.posId, curData.value)
          PaxosPrepareOk(curData)
        } else {
          PaxosPrepareDeny()
        }
      }
    } else {
      curData = PaxosData(data.round, data.posId, null)
      PaxosPrepareOk(curData)
    }

    isWorking.compareAndSet(true, false)

    log.info(s"onPrepare end : new data=${curData.round},${curData.posId},${PaxosUtils.getString(curData.value)}")

    out
  }

  override def onAccept(data: PaxosData): Any = {
    if (curData == null){
      log.info(s"onAccept start : cur=null, new=${data.round},${data.posId},${PaxosUtils.getString(data.value)}")
    } else {
      log.info(s"onAccept start : cur=${curData.round},${curData.posId},${PaxosUtils.getString(curData.value)}, new=${data.round},${data.posId},${PaxosUtils.getString(data.value)}")
    }

    if (data == null || data.value == null) {
      throw new PaxosException("accept value should not null")
    }

    while(!isWorking.compareAndSet(false, true)) {
      //
    }

    val out = if (curData != null) {
      if (curData.round < data.round) {
        curData = data
        PaxosAcceptDeny()
      } else if (curData.round > data.round) {
        PaxosAcceptDeny()
      } else {
        if (curData.posId <= data.posId) {
          curData = data
          PaxosAcceptOk(curData)
        } else {
          PaxosAcceptDeny()
        }
      }
    } else {
      curData = data
      PaxosAcceptOk(curData)
    }
    isWorking.compareAndSet(true, false)

    log.info(s"onAccept end : new data=${curData.round},${curData.posId},${PaxosUtils.getString(curData.value)}")

    out
  }

  override def start(): Unit = {

  }
  override def stop(): Unit = {

  }
}
