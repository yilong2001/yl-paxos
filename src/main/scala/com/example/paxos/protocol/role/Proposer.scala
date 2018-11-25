package com.example.paxos.protocol.role

import com.example.paxos.protocol.endpoint.PaxosData
import com.example.paxos.protocol.exector.CompletionStatus.CompletionStatus

import scala.concurrent.Future

/**
  * Created by yilong on 2018/11/8.
  */

trait Proposer {
  def onRequest(bytes : Array[Byte]) : Future[Any]
  def complete(paxosData: PaxosData, overRound : Long, isLocal : Boolean) : CompletionStatus

  def getCurProposalId(round : Long, retry : Long) : Long

  def start() : Unit
  def stop() : Unit
}
