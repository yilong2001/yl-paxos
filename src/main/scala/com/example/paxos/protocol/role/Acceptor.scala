package com.example.paxos.protocol.role

import com.example.paxos.protocol
import com.example.paxos.protocol._
import com.example.paxos.protocol.endpoint.PaxosData

/**
  * Created by yilong on 2018/11/8.
  */
trait Acceptor {
  def onPrepare(data: PaxosData) : Any
  def onAccept(data: PaxosData) : Any

  def start() : Unit
  def stop() : Unit
}

