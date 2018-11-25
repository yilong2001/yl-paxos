package com.example.paxos.protocol.role

import com.example.jrpc.nettyrpc.rpc.HostPort

/**
  * Created by yilong on 2018/11/24.
  */
trait DataSyncer {
  def onSayHello(round : Long, hostPort: HostPort) : Any
  def onSyncReq(start : Long, end : Long) : Any

  def start() : Unit
  def stop() : Unit
}
