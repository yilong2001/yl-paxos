package com.example.paxos.protocol.exector

import com.example.paxos.instance.PaxosInstance
import com.example.paxos.protocol.procedure.ProcedureCommit
import com.example.paxos.protocol.role.Learner
import com.example.paxos.protocol.endpoint.{PaxosChosenOk, PaxosData}
import com.example.paxos.util.PaxosUtils
import org.apache.commons.logging.LogFactory

/**
  * Created by yilong on 2018/11/16.
  */
class DefaultLearner(instance : PaxosInstance) extends Learner{
  private val log = LogFactory.getLog(classOf[DefaultLearner])

  val commitExec : ProcedureCommit = new ProcedureCommit(this, instance)

  override def commit(data: PaxosData): Boolean = {
    log.info(s"start commit : ${data.round},${data.posId},${PaxosUtils.getString(data.value)}")
    instance.getStore().write(data)
    commitExec.commit(data)
  }

  override def onChosen(data: PaxosData): Any = {
    log.info(s"start onChosen : ${data.round},${data.posId},${PaxosUtils.getString(data.value)}")

    instance.getProposer().complete(data, data.round, false)
    instance.getStore().write(data)
    PaxosChosenOk()
  }

  override def start(): Unit = {

  }
  override def stop(): Unit = {

  }
}
