package com.example.paxos.client

/**
  * Created by yilong on 2018/11/9.
  */

case class PaxosClientReq(bytes : Array[Byte])

case class PaxosClientReply(round : Long, bytes : Array[Byte])



