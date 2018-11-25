package com.example.paxos.protocol.exector

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.example.paxos.client.PaxosClientReply
import com.example.paxos.exception.PaxosException
import com.example.paxos.instance.PaxosInstance
import com.example.paxos.protocol.endpoint.PaxosData
import com.example.paxos.protocol.exector.CompletionStatus.CompletionStatus
import com.example.paxos.protocol.procedure.{ProcedureAccept, ProcedurePrepare}
import com.example.paxos.protocol.role.Proposer
import com.example.paxos.protocol.util.ThreadServiceExecutor
import com.example.paxos.util.PaxosUtils
import org.apache.commons.logging.LogFactory

import scala.concurrent.{Future, Promise}
import scala.util.control.Breaks
import scala.util.{Failure, Success}

/**
  * Created by yilong on 2018/11/8.
  *
  * for simple, do not support dynamically add node(instance)
  */
class DefaultProposer(val instance : PaxosInstance,
                      val idBase : Long,
                      val idOffset : Long) extends Proposer {
  private val log = LogFactory.getLog(classOf[DefaultProposer])

  var curRound = new AtomicLong(idBase)

  val isWorking = new AtomicBoolean(false)
  val isCompleting = new AtomicBoolean(false)

  // @volatile var consensusThreadFuture : java.util.concurrent.Future[Boolean] = null
  // @volatile var resultPromise : Promise[Any] = null
  val outputsPerRound = new ConcurrentHashMap[Long, ProposerOutput]()

  def getCurProposalId(round : Long, retry : Long) : Long =
    (retry) * instance.getAllAddrs().size + idOffset

  /*
  * asyc reply
  *
  * */
  //TODO: start, how to set round ???
  override def onRequest(bytes : Array[Byte]) : Future[Any] = {
    val round = instance.getCurrentRound()
    log.info(s"onRequest at  ${round}, ${new String(bytes, "utf-8")}")

    if (!isWorking.compareAndSet(false, true)) {
      Future{
        Failure(new PaxosException(s"paxos ${round} is working"))
      }(ThreadServiceExecutor.execContext)
    } else {
      curRound.set(round)

      val output = startConsensus(round, bytes)
      outputsPerRound.put(round, output)

      output.reply.future
    }
  }

  /*
  * complete procedure
  * proposer complete --> local learner chosen data --> notify other learners * remoteChosen *
  * --> other proposers complete
  * */
  override def complete(paxosData: PaxosData, overRound : Long, isLocal : Boolean) : CompletionStatus = {
    val loop = new Breaks
    var status = (paxosData==null) match {case true => CompletionStatus.Failed; case _ => CompletionStatus.Success}

    loop.breakable({
      val crd = curRound.get()
      if (paxosData != null) log.info(s"start complete(data) : ${paxosData.round},${paxosData.posId},${PaxosUtils.getString(paxosData.value)}")
      log.info(s"start complete : rd=${overRound}, local=${isLocal}, curRd=${crd}, working=${isWorking.get()}, completing=${isCompleting.get()}")
      if (overRound < crd) {
        //directly skip
        status = CompletionStatus.Skipped
        loop.break()
      } else if (overRound > crd) {
        //TODO: exception need sync data, instance should do sth
        log.error(s"overRound (${overRound}) is bigger than crd (${crd})")
        status = CompletionStatus.Failed
        loop.break()
      } else {
        None
      }

      if (!isWorking.get()) { loop.break() }
      if (!isCompleting.compareAndSet(false, true)) {loop.break()}
      if (stopConsensus(overRound)) {
        isWorking.compareAndSet(true, false)
        if (isLocal && paxosData != null) {
          instance.getLearner().commit(paxosData)
        }
      }
      isCompleting.compareAndSet(true, false)
    })

    status
  }

  def startConsensus(round : Long, bytes : Array[Byte]) : ProposerOutput = {
    var promise = Promise[Any]()
    log.info("startConsensus at  "+round)

    val run = new Runnable {
      override def run(): Unit = {
        execConsensus(promise,round,bytes)
      }
    }

    ThreadServiceExecutor.execContext.execute(run)
    ProposerOutput(promise, null)
  }

  def stopConsensus(round : Long) : Boolean = {
    val output = outputsPerRound.remove(round)
    log.info(s"stopConsensus round : ${round}, ${output==null}")
    if (output != null) {
      if (output.reply != null && !output.reply.isCompleted) {
        output.reply.tryFailure(new PaxosException("completed by other while stop consensus"))
      }

      if (output.task != null) {
        output.task.cancel(true)
      }

      true
    } else {
      false
    }
  }

  def execConsensus(pout : Promise[Any], round : Long, bytes : Array[Byte]) : Boolean = {
    val loop = new Breaks

    val mainRetry = new RetryCount(0, instance.getMaxRetryCount())

    var result = true
    var paxosData : PaxosData = null
    log.info(s"start execConsensus : ${round}")

    val prepareExec = ProcedurePrepare(this, instance)
    val acceptExec = ProcedureAccept(instance)
    var allRetrys : Long = 0

    loop.breakable({
      while(!mainRetry.isCountExpired()) {
        val subRetry = new RetryCount(allRetrys, instance.getMaxRetryCount())
        result = false
        //if has been completed
        if (!outputsPerRound.containsKey(round)) {
          log.error("outputsPerRound not contain : "+round+", "+mainRetry.getCount+" ")
          result = false
          loop.break()
        }

        val preok = prepareExec.syncRetry(round, subRetry)

        allRetrys = subRetry.getCount()

        log.info(s"prepare retry result : ${round}, ${allRetrys}, ${result}, ${preok}")
        if (preok == null) {
          result = false
          loop.break()
        }

        //if has been completed
        if (!outputsPerRound.containsKey(round)) {
          result = false
          loop.break()
        }

        paxosData = PaxosData(preok.data.round, preok.data.posId, ((preok.data.value == null) match {
          case true => bytes
          case _ => preok.data.value
        }))

        result = acceptExec.accept(paxosData)
        if (result) {
          result = true
          loop.break()
        }

        mainRetry.increaseCount()
      }
    })

    result match {
      case true if (paxosData != null && paxosData.value != null && paxosData.value.deep == bytes.deep) =>
        pout.trySuccess(new PaxosClientReply(round, paxosData.value))
      case _ => pout.tryFailure(new PaxosException(s"propose retry (${round}, ${mainRetry.getCount()}) failed"))
    }

    //结束本轮 Paxos
    complete(paxosData, round, true)

    if (paxosData != null) {
      log.info(s"execConsensus complete : ${round}, ${paxosData.round},${paxosData.posId},${PaxosUtils.getString(paxosData.value)}")
    } else {
      log.info(s"execConsensus complete : ${round}, data = null")
    }

    result
  }

  override def start(): Unit = {

  }
  override def stop(): Unit = {
    complete(null, curRound.get(), false)
  }
}
