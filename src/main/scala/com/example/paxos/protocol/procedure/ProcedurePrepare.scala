package com.example.paxos.protocol.procedure

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

import com.example.paxos.exception.PaxosException
import com.example.paxos.instance.PaxosInstance
import com.example.paxos.protocol.endpoint.{PaxosData, PaxosPrepare, PaxosPrepareDeny, PaxosPrepareOk}
import com.example.paxos.protocol.exector.RetryCount
import com.example.paxos.protocol.role.Proposer
import com.example.paxos.protocol.util.ThreadServiceExecutor
import com.example.paxos.util.PaxosUtils
import com.example.srpc.nettyrpc.util.ThreadUtils
import org.apache.commons.logging.LogFactory

import scala.concurrent.Promise
import scala.concurrent.duration.Duration
import scala.util.control.Breaks
import scala.util.{Failure, Success}

/**
  * Created by yilong on 2018/11/17.
  */
case class ProcedurePrepare(val proposer : Proposer, val instance : PaxosInstance) {
  private val log = LogFactory.getLog(classOf[ProcedurePrepare])

  def syncRetry(round : Long, retryCount: RetryCount) : PaxosPrepareOk = {
    val promise = Promise[Any]()
    def onFailure(e: Throwable): Unit = {
      if (!promise.tryFailure(e)) {
        log.error(e)
      }
    }

    def onSuccess(preOk : PaxosPrepareOk): Unit = {
      promise.trySuccess(preOk)
    }

    val loop = new Breaks
    loop.breakable({
      val schd = schedule(retryCount, round, onSuccess, onFailure)
      try {
        if (schd.get(instance.getBaseTimeoutInterval*2, TimeUnit.MILLISECONDS).asInstanceOf[Boolean]) {
          loop.break()
        } else {
          //continue
        }
      } catch {
        case e : Exception => log.error(e)
      }
    })

    ThreadUtils.awaitResult[Any](promise.future,
      Duration(instance.getBaseTimeoutInterval, TimeUnit.MILLISECONDS))

    val preok : PaxosPrepareOk = promise.future.value
      .getOrElse(Failure(new PaxosException("unkonw reason on prepare when waiting"))) match {
      case Success(pok) => pok.asInstanceOf[PaxosPrepareOk]
      case Failure(e) => log.error(e); null
      case _ => log.error("error reply on prepare when waiting ... "); null
    }

    preok
  }

  def prepare(round : Long, curProsid : Long): (Boolean, Array[Byte]) = {
    //只需要收到过半数的成功响应，就可以继续下一步
    val latch = new java.util.concurrent.CountDownLatch(instance.getConsensusThreshold())
    val targetValue = new AtomicReference[(Long, Array[Byte])]((0, null))

    instance.getAcceptorRefs(true).map(acceptor => {
      val f = ThreadServiceExecutor[Any]({
        try {
          val rsp = acceptor.askSync[Any](PaxosPrepare(PaxosData(round, curProsid, null)),
            instance.getBaseTimeoutInterval())
          log.info(s" acceptor reply prepare ${rsp}")
          rsp match {
            case PaxosPrepareOk(data) => (PaxosPrepareOk(data))
            case _ => log.info("acceptor reply error : "); throw new PaxosException("prepare reply not ok")
          }
        } catch {
          case e : Exception => { log.error(e.getMessage); throw new PaxosException(e.getMessage) }
        }
      })(ThreadServiceExecutor.execContext)

      f.onComplete(info => info match {
        case Success(PaxosPrepareOk(PaxosData(rd, posId, value))) => {
          if (rd != round) {
            //TODO : round is different
            log.error(s"prepare paxos failed, round is different : (${round}) : ${rd}, ${posId}, ${new String(value,"utf-8")}")
          } else {
            if (value != null) {
              var break = false
              while(!break) {
                val tmp = targetValue.get()
                if (posId > tmp._1) {
                  if (targetValue.compareAndSet(tmp, (posId, value))) {
                    break = true
                  }
                } else {
                  //TODO: if value is different with target value, exception
                  break = true
                }
              }
            }

            log.info(s"prepare paxos success : ${round}, ${posId}, ${PaxosUtils.getString(value)}")
            latch.countDown()
          }
        }
        case Success(PaxosPrepareDeny) => log.info("receive PaxosPrepareDeny ")
        case Failure(e) => log.error(e.getMessage);
        case _ => log.error(s"unknowed PaxosPrepare reply : ${info}");
      })(ThreadServiceExecutor.execContext)

      f
    })

    //TODO: ???
    //只收集多数的响应结果，选择出来的非null value，一定是经过多数 pre表决的 value ???
    val isOk = latch.await(instance.getBaseTimeoutInterval(), java.util.concurrent.TimeUnit.MILLISECONDS)

    //success or failed
    (isOk, targetValue.get()._2)
  }

  def schedule(retryCount : RetryCount,
               round : Long,
               onSuccess: (PaxosPrepareOk) => Unit,
               onFailure: (Throwable) => Unit) : java.util.concurrent.ScheduledFuture[_] = {
    val call = new java.util.concurrent.Callable[Boolean] {
      override def call(): Boolean = {
        try {
          if (retryCount.isCountExpired()) {
            onFailure(new PaxosException("too much retry"))
            true
          } else {
            retryCount.increaseCount()
            val curposid = proposer.getCurProposalId(round, retryCount.getCount())
            prepare(round, curposid) match {
              case (true, value) => onSuccess(PaxosPrepareOk(PaxosData(round, curposid, value))); true
              case _ => false;
            }
          }
        } catch {
          case e : Exception => {
            log.error(e)
            onFailure(e)
            true
          }
        }
      }
    }

    ThreadServiceExecutor.proposerTimerSchd.schedule[Boolean](call,
      (retryCount.getCount()==0) match {
        case true => 0
        case _ => instance.getTimeoutInterval
      },
      TimeUnit.MILLISECONDS)

    //PaxosUtils.schedulePeriodly[Boolean](PaxosExecutor.proposerTimerSchd, call, instance.getTimeoutInterval)
  }
}
