package com.example.paxos.protocol.endpoint

import com.example.jrpc.nettyrpc.rpc.HostPort

/**
  * Created by yilong on 2018/11/8.
  */
case class PeerAddr(hostPort: HostPort, serviceName : String)

case class PaxosData(round : Long, posId : Long, value : Array[Byte])

case class PaxosPrepare(data : PaxosData) {}

case class PaxosPrepareOk(data : PaxosData/*round : Long, posId : Long, value : Array[Byte]*/)
case class PaxosPrepareDeny()

case class PaxosAccept(data : PaxosData) {}

case class PaxosAcceptOk(data : PaxosData)
case class PaxosAcceptDeny()

case class PaxosChosen(data : PaxosData/*round : Long, bytes : Array[Byte]*/) {}

case class PaxosChosenOk() {}
case class PaxosChosenDeny() {}

case class PaxosSyncHello(round : Long, hostPort: HostPort)
case class PaxosSyncReq(start : Long, end : Long)
case class PaxosSyncReply(datas : List[PaxosData])
