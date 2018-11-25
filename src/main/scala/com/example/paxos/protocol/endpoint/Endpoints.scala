package com.example.paxos.protocol.endpoint

import com.example.jrpc.nettyrpc.rpc.HostPort
import com.example.paxos.client.PaxosClientReq
import com.example.paxos.instance.{DefaultInstance, PaxosInstance}
import com.example.paxos.protocol.role.{Acceptor, DataSyncer, Learner, Proposer}
import com.example.paxos.protocol.util.ThreadServiceExecutor
import com.example.paxos.protocol._
import com.example.srpc.nettyrpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.commons.logging.LogFactory

import scala.util.{Failure, Success}

/**
  * Created by yilong on 2018/11/19.
  */
class ProposerEndpoint(override val rpcEnv: RpcEnv,
                       override val epName : String,
                       val proposer : Proposer) extends RpcEndpoint {
  private val log = LogFactory.getLog(classOf[ProposerEndpoint])

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PaxosClientReq(bytes) => {
      val f = proposer.onRequest(bytes)
      f.onComplete(info => info match {
        case Success(rsp) => context.reply(rsp)
        case Failure(e) => context.sendFailure(e)
      })(ThreadServiceExecutor.execContext)
    }
  }

  override def onStart() : Unit = {
    log.info("start ProposerEndpoint endpoint")
  }

  override def onStop(): Unit = {
    log.info("stop ProposerEndpoint endpoint")
  }
}

class AcceptorEndpoint(override val rpcEnv: RpcEnv,
                       override val epName : String,
                       val acceptor: Acceptor) extends RpcEndpoint {
  private val log = LogFactory.getLog(classOf[ProposerEndpoint])

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PaxosPrepare(data) => {
      context.reply(acceptor.onPrepare(data))
    }
    case PaxosAccept(data) => {
      context.reply(acceptor.onAccept(data))
    }
  }

  override def onStart() : Unit = {
    log.info("start AcceptorEndpoint endpoint")
  }

  override def onStop(): Unit = {
    log.info("stop AcceptorEndpoint endpoint")
  }
}

class LearnerEndpoint(override val rpcEnv: RpcEnv,
                      override val epName : String,
                      val learner: Learner) extends RpcEndpoint {
  private val log = LogFactory.getLog(classOf[LearnerEndpoint])

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PaxosChosen(data) => {
      context.reply(learner.onChosen(data))
    }
  }

  override def onStop(): Unit = {
    log.info("stop LearnerEndpoint endpoint")
  }

  override def onStart() : Unit = {
    log.info("start LearnerEndpoint endpoint")
  }
}

class DataSyncerEndpoint(override val rpcEnv: RpcEnv,
                      override val epName : String,
                      val dataSyncer: DataSyncer) extends RpcEndpoint {
  private val log = LogFactory.getLog(classOf[DataSyncerEndpoint])

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case PaxosSyncHello(round, hostPort) => {
      context.reply(dataSyncer.onSayHello(round, hostPort))
    }
    case PaxosSyncReq(start, end) => {
      context.reply(dataSyncer.onSyncReq(start, end))
    }
  }

  override def onStop(): Unit = {
    log.info("stop DataSyncEndpoint endpoint")
  }

  override def onStart() : Unit = {
    log.info("start DataSyncEndpoint endpoint")
  }

}
