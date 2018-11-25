package com.example.paxos.server

import com.example.jrpc.nettyrpc.rpc.RpcConfig
import com.example.paxos.instance.DefaultInstance
import com.example.paxos.io.DefaultStore
import com.example.paxos.protocol.endpoint.{AcceptorEndpoint, DataSyncerEndpoint, LearnerEndpoint, ProposerEndpoint}
import com.example.paxos.protocol.exector.{DefaultAcceptor, DefaultDataSyncer, DefaultLearner, DefaultProposer}
import com.example.paxos.protocol.util.ThreadServiceExecutor
import com.example.paxos.util.PaxosConfig
import com.example.srpc.nettyrpc.{RpcEndpoint, RpcEnv}

/**
  * Created by yilong on 2018/11/8.
  */
object PaxosServer extends App {
  val defaultHost = "localhost"
  val defaultPort = 12201
  val defaultDir = "/Users/yilong/research/data/paxos"

  def buildConfig() : PaxosConfig = {
    val config = new PaxosConfig
    val BaseTimeoutInterval = 200; //ms
    val MaxRetryCount = 10

    config.set("paxos.base.timeout.intervale", BaseTimeoutInterval)
    config.set("paxos.max.retry.count", MaxRetryCount)

    config.set("paxos.acceptor.rpc.service.name", "acceptor")
    config.set("paxos.proposer.rpc.service.name", "proposer")
    config.set("paxos.learner.rpc.service.name", "learner")
    config.set("paxos.datasyncer.rpc.service.name", "syncer")

    config.set("paxos.datasyncer.peer.num", 1)

    config
  }

  def startApplication() = {
    if (this.args.length < 6) {
      println("will use defaultHost : $defaultHost, defaultPort : $defaultPort")
      println("com.example.paxos.PaxosServer host port offset allhp basedir")
    }

    this.args.zipWithIndex.foreach((tuple:(String, Int))=>println(tuple._2+" = "+tuple._1))

    val host = this.args.length >= 1 match {
      case true => this.args(0)
      case _ => defaultHost
    }

    val port = this.args.length >= 2 match {
      case true => this.args(1).toInt
      case _ => defaultPort
    }

    val offset = this.args.length >= 3 match {
      case true => this.args(2).toInt
      case _ => 0
    }

    val hps = this.args.length >= 4 match {
      case true => this.args(3)
      case _ => "localhost:12201,localhost:12202,localhost:12203"
    }

    val basedir = this.args.length >= 5 match {
      case true => this.args(4)
      case _ => defaultDir
    }

    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), host, port, false)

    val config = buildConfig
    config.set("paxos.hostport.all", hps)
    config.set("paxos.hostport.local", host+":"+port)
    config.set("paxos.data.base.dir", basedir)

    val paxosInstance = new DefaultInstance(rpcEnv, config)

    val store = new DefaultStore(paxosInstance)
    paxosInstance.setStore(store)
    store.start()

    val proposer = new DefaultProposer(paxosInstance,
      store.getLastRound() * paxosInstance.getAllAddrs().size,
      offset)
    paxosInstance.setProposer(proposer)
    proposer.start()

    val acceptor = new DefaultAcceptor(paxosInstance)
    paxosInstance.setAcceptor(acceptor)
    acceptor.start()

    val learner = new DefaultLearner(paxosInstance)
    paxosInstance.setLearner(learner)
    learner.start()

    val dataSyncer = new DefaultDataSyncer(paxosInstance, config.get("paxos.datasyncer.peer.num").toString.toInt)
    paxosInstance.setDataSyncer(dataSyncer)
    dataSyncer.start()

    val proposerEndpoint: RpcEndpoint = new ProposerEndpoint(rpcEnv,
      paxosInstance.getProposerRpcServiceName(),
      proposer)

    val acceptorEndpoint: RpcEndpoint = new AcceptorEndpoint(rpcEnv,
      paxosInstance.getAcceptorRpcServiceName(),
      acceptor)

    val learnerEndpoint: RpcEndpoint = new LearnerEndpoint(rpcEnv,
      paxosInstance.getLearnerRpcServiceName(),
      learner)

    val dataSyncerEndpoint: RpcEndpoint = new DataSyncerEndpoint(rpcEnv,
      paxosInstance.getDataSyncerRpcServiceName(),
      dataSyncer)

    rpcEnv.setupEndpoint(paxosInstance.getProposerRpcServiceName(), proposerEndpoint)

    rpcEnv.setupEndpoint(paxosInstance.getAcceptorRpcServiceName(), acceptorEndpoint)

    rpcEnv.setupEndpoint(paxosInstance.getLearnerRpcServiceName(), learnerEndpoint)

    rpcEnv.setupEndpoint(paxosInstance.getDataSyncerRpcServiceName(), dataSyncerEndpoint)

    rpcEnv.awaitTermination()

    ThreadServiceExecutor.stop()
  }

  startApplication()
}
