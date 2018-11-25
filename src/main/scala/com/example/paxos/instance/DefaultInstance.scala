package com.example.paxos.instance
import java.util.concurrent.ConcurrentHashMap

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig}
import com.example.paxos.io.Store
import com.example.paxos.protocol.role.{Acceptor, DataSyncer, Learner, Proposer}
import com.example.paxos.util.{PaxosConfig, PaxosUtils}
import com.example.srpc.nettyrpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.commons.logging.LogFactory

import scala.util.Random

/**
  * Created by yilong on 2018/11/12.
  * multi instance : 多进程
  */
class DefaultInstance(val rpcEnv: RpcEnv,
                      val config : PaxosConfig) extends PaxosInstance {
  private val log = LogFactory.getLog(classOf[DefaultInstance])

  private var proposer: Proposer = null
  private var acceptor: Acceptor = null
  private var learner: Learner = null
  private var store: Store = null
  private var datasyncer: DataSyncer = null


  private val allAddrs = (config.get("paxos.hostport.all")).toString
    .trim.split(",").toList.map(hp => {
    val hostPortArr = hp.trim.split(":")
    new HostPort(hostPortArr(0), (hostPortArr(1).toInt))
  })

  allAddrs.foreach(hp => { println(hp.hostPort())})

  private val localAddr = {
    val hostPortArr = config.get("paxos.hostport.local").toString.split(":")
    new HostPort(hostPortArr(0), (hostPortArr(1).toInt))
  }

  private val allAddrsWithoutLocal = allAddrs.filter(hp => !hp.hostPort().equals(localAddr.hostPort()))

  def getAllAddrs() : List[HostPort] = {
    allAddrs
  }

  def getLocalAddr() : HostPort = {
    localAddr
  }

  override def getProposerRefs(withLocal : Boolean): List[RpcEndpointRef] = {
    //TODO: maybe cache RpcEndpointRef
    null
  }

  val acceptorEndpointRefMap = new ConcurrentHashMap[HostPort, RpcEndpointRef]()
  override def getAcceptorRefs(withLocal : Boolean): List[RpcEndpointRef] = {
    //TODO: cache RpcEndpointRef
    allAddrs.filter(hp=>{
      if (withLocal) true
      else !hp.hostPort().equals(getLocalAddr().hostPort())
    }).map(hp => {
      if (acceptorEndpointRefMap.get(hp) != null) {
        acceptorEndpointRefMap.get(hp)
      } else {
        val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(hp, getAcceptorRpcServiceName())

        if (endPointRef != null) {
          acceptorEndpointRefMap.put(hp, endPointRef)
        }
        endPointRef
      }
    })
  }

  def getAcceptorRpcEndpointRef(hostPort: HostPort) : RpcEndpointRef = {
    if (!acceptorEndpointRefMap.contains(hostPort)) {
      getAcceptorRefs(true)
    }
    acceptorEndpointRefMap.get(hostPort)
  }

  val learnerEndpointRefMap = new ConcurrentHashMap[HostPort, RpcEndpointRef]()
  override def getLearnerRefs(withLocal : Boolean): List[RpcEndpointRef] = {
    allAddrsWithoutLocal.filter(hp=>{
      if (withLocal) true
      else !hp.hostPort().equals(getLocalAddr().hostPort())
    }).map(hp => {
      if (learnerEndpointRefMap.get(hp) != null) {
        learnerEndpointRefMap.get(hp)
      } else {
        val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(hp, getLearnerRpcServiceName())

        if (endPointRef != null) {
          learnerEndpointRefMap.put(hp, endPointRef)
        }
        endPointRef
      }
    })
  }

  def getLearnerRpcEndpointRef(hostPort: HostPort) : RpcEndpointRef = {
    if (!learnerEndpointRefMap.contains(hostPort)) {
      getLearnerRefs(true)
    }
    learnerEndpointRefMap.get(hostPort)
  }

  val dataSyncerEndpointRefMap = new ConcurrentHashMap[HostPort, RpcEndpointRef]()
  override def getDataSyncerRefs(withLocal : Boolean): List[RpcEndpointRef] = {
    //TODO: cache RpcEndpointRef
    allAddrs.filter(hp=>{
      if (withLocal) true
      else !hp.hostPort().equals(getLocalAddr().hostPort())
    }).map(hp => {
      if (dataSyncerEndpointRefMap.get(hp) != null) {
        dataSyncerEndpointRefMap.get(hp)
      } else {
        val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(hp, getDataSyncerRpcServiceName())

        if (endPointRef != null) {
          dataSyncerEndpointRefMap.put(hp, endPointRef)
        }
        endPointRef
      }
    })
  }
  override def getDataSyncerRpcEndpointRef(hostPort: HostPort) : RpcEndpointRef = {
    //if (!dataSyncerEndpointRefMap.contains(hostPort) || (dataSyncerEndpointRefMap.get(hostPort) == null)) {
    //  getDataSyncerRefs(true)
    //}
    var ref = dataSyncerEndpointRefMap.get(hostPort)
    if (ref == null) {
      ref = rpcEnv.setupEndpointRef(hostPort, getDataSyncerRpcServiceName())
      if (ref != null) dataSyncerEndpointRefMap.put(hostPort, ref)
    }

    ref
  }

  override def getProposer() : Proposer = {
    proposer
  }

  override def getLearner() : Learner = {
    learner
  }

  override def getAcceptor() : Acceptor = {
    acceptor
  }

  override def getStore(): Store = {
    store
  }

  override def setStore(store: Store) : Unit = {
    this.store = store
  }

  override def setProposer(proposer: Proposer) : Unit = {
    this.proposer = proposer
  }
  override def setLearner(learner: Learner) : Unit = {
    this.learner = learner
  }
  override def setAcceptor(acceptor: Acceptor) : Unit = {
    this.acceptor = acceptor
  }

  def setDataSyncer(syncer: DataSyncer) : Unit = {
    this.datasyncer = syncer
  }

  override def getDataSyncer(): DataSyncer = {
    this.datasyncer
  }

  override def getCurrentRound(): Long = {
    store.getLastRound() + 1
  }

  log.info("consensus threahold is : " + PaxosUtils.consensusThreshold(allAddrs.size))
  override def getConsensusThreshold(): Int = {
    PaxosUtils.consensusThreshold(allAddrs.size)
  }

  def getTimeoutInterval() : Int = {
    getBaseTimeoutInterval + new Random(System.currentTimeMillis()).nextInt(getBaseTimeoutInterval)
  }

  def getBaseTimeoutInterval() : Int = {
    config.get("paxos.base.timeout.intervale").asInstanceOf[Int]
  }

  def getMaxRetryCount() : Int = {
    config.get("paxos.max.retry.count").asInstanceOf[Int]
  }

  def getBaseDir() : String = {
    config.get("paxos.data.base.dir").toString+"/"+getLocalAddr().getHost+"-"+getLocalAddr().getPort
  }

  def getPaxosConfig() : PaxosConfig = {
    config
  }

  def getAcceptorRpcServiceName() : String = {
    config.get("paxos.acceptor.rpc.service.name").toString
  }

  def getProposerRpcServiceName() : String = {
    config.get("paxos.proposer.rpc.service.name").toString
  }

  def getLearnerRpcServiceName() : String = {
    config.get("paxos.learner.rpc.service.name").toString
  }

  def getDataSyncerRpcServiceName() : String = {
    config.get("paxos.datasyncer.rpc.service.name").toString
  }
}
