package com.example.paxos.protocol.procedure

import com.example.paxos.exception.PaxosException
import com.example.paxos.instance.PaxosInstance
import com.example.paxos.protocol.endpoint.{PaxosAccept, PaxosAcceptOk, PaxosData}
import com.example.paxos.protocol.util.ThreadServiceExecutor
import com.example.paxos.util.PaxosUtils
import org.apache.commons.logging.LogFactory

import scala.util.{Failure, Success}

/**
  * Created by yilong on 2018/11/22.
  */
case class ProcedureAccept(val instance : PaxosInstance) {
  private val log = LogFactory.getLog(classOf[ProcedureAccept])

  def accept(paxosData: PaxosData): Boolean = {
    log.info(s"start acceptOnce : data=${paxosData.round},${paxosData.posId},${PaxosUtils.getString(paxosData.value)}")

    val latch = new java.util.concurrent.CountDownLatch(instance.getConsensusThreshold())
    val tmp = instance.getAcceptorRefs(true).map(acceptor => {
      val f = ThreadServiceExecutor[Any]({
        try {
          val rsp = acceptor.askSync[Any](PaxosAccept(paxosData), instance.getBaseTimeoutInterval)
          log.info(s"************ paxos acceptOnce reply :  ${rsp} ************* ");
          rsp match {
            case PaxosAcceptOk(data) => rsp
            case _ => throw (new PaxosException("accept reply not ok"))
          }
        } catch {
          case e : Exception => { e.printStackTrace();  throw new PaxosException(e.getMessage) }
        }
      })(ThreadServiceExecutor.execContext)

      f.onComplete(info => info match {
        case Success(msg) => latch.countDown()
        case Failure(e) => log.error(e);
      })(ThreadServiceExecutor.execContext)

      f
    })

    val isok = latch.await(instance.getBaseTimeoutInterval, java.util.concurrent.TimeUnit.MILLISECONDS)
    log.info(s"end acceptOnce : result = ${isok}")

    isok
  }
}
