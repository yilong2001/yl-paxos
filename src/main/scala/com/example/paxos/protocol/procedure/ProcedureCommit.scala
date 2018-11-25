package com.example.paxos.protocol.procedure

import com.example.paxos.exception.PaxosException
import com.example.paxos.instance.PaxosInstance
import com.example.paxos.protocol.role.Learner
import com.example.paxos.protocol.util.ThreadServiceExecutor
import com.example.paxos.protocol.endpoint.{PaxosChosen, PaxosData}
import org.apache.commons.logging.LogFactory

import scala.util.{Failure, Success}

/**
  * Created by yilong on 2018/11/17.
  */
class ProcedureCommit(val learner : Learner,
                      val instance : PaxosInstance) {
  private val log = LogFactory.getLog(classOf[ProcedureCommit])

  def commit(paxosData: PaxosData) : Boolean = {
    //TODO: save locally and disk flush
    val latch = new java.util.concurrent.CountDownLatch(instance.getConsensusThreshold() - 1)

    val ps = instance.getLearnerRefs(false).map(p => {
      val f = ThreadServiceExecutor({
        try {
          //TODO: chosen data
          val rsp = p.askSync(PaxosChosen(paxosData), instance.getBaseTimeoutInterval())
          log.info(s"Commit reply : ${rsp}")
          rsp
        } catch {
          case e : Exception => log.error(e); Failure(new PaxosException("broadcast chosed failed"))
        }
      })(ThreadServiceExecutor.execContext)

      f.onComplete(info => info match {
        case Success(rsp) => latch.countDown()
        case Failure(e) => println(e)
      })(ThreadServiceExecutor.execContext)

      f
    })

    latch.await(instance.getBaseTimeoutInterval(), java.util.concurrent.TimeUnit.MILLISECONDS)
  }
}
