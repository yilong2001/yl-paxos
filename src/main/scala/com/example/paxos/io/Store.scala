package com.example.paxos.io

import com.example.paxos.protocol.endpoint.PaxosData

/**
  * Created by yilong on 2018/11/18.
  */
trait Store {
  def write(data : PaxosData)
  def write(datas : List[PaxosData])
  def read(round : Long) : PaxosData
  def read(start : Long, end : Long) : List[PaxosData]
  def getLastRound() : Long
  def start() : Unit
}
