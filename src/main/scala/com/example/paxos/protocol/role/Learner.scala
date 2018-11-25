package com.example.paxos.protocol.role

import com.example.paxos.protocol.endpoint.PaxosData

/**
  * Created by yilong on 2018/11/8.
  */
trait Learner {
  def commit(data: PaxosData) : Boolean
  def onChosen(data: PaxosData) : Any

  def start() : Unit
  def stop() : Unit
}

