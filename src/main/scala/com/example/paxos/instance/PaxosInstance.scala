package com.example.paxos.instance

import com.example.jrpc.nettyrpc.rpc.HostPort
import com.example.paxos.io.Store
import com.example.paxos.protocol.role.{Acceptor, DataSyncer, Learner, Proposer}
import com.example.paxos.util.PaxosConfig
import com.example.srpc.nettyrpc.RpcEndpointRef

/**
  * Created by yilong on 2018/11/12.
  */
trait PaxosInstance {
  def getAllAddrs() : List[HostPort]
  def getLocalAddr() : HostPort

  def getProposerRefs(withLocal : Boolean) : List[RpcEndpointRef]
  def getAcceptorRefs(withLocal : Boolean) : List[RpcEndpointRef]
  def getLearnerRefs(withLocal : Boolean) : List[RpcEndpointRef]
  def getDataSyncerRefs(withLocal : Boolean) : List[RpcEndpointRef]

  def getProposer() : Proposer
  def getLearner() : Learner
  def getAcceptor() : Acceptor
  def getStore() : Store
  def getDataSyncer(): DataSyncer

  def setProposer(proposer: Proposer) : Unit
  def setLearner(learner: Learner) : Unit
  def setAcceptor(acceptor: Acceptor) : Unit
  def setStore(store: Store) : Unit

  def getCurrentRound() : Long

  def getConsensusThreshold() : Int

  def getTimeoutInterval() : Int
  def getBaseTimeoutInterval() : Int
  def getMaxRetryCount() : Int

  def getAcceptorRpcEndpointRef(hostPort: HostPort) : RpcEndpointRef
  def getLearnerRpcEndpointRef(hostPort: HostPort) : RpcEndpointRef
  def getDataSyncerRpcEndpointRef(hostPort: HostPort) : RpcEndpointRef


  def getPaxosConfig() : PaxosConfig

  def getBaseDir() : String

  def getAcceptorRpcServiceName() : String
  def getProposerRpcServiceName() : String
  def getLearnerRpcServiceName() : String
  def getDataSyncerRpcServiceName() : String

}
