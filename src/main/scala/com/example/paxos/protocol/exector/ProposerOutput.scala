package com.example.paxos.protocol.exector

import scala.concurrent.Promise

/**
  * Created by yilong on 2018/11/22.
  */
case class ProposerOutput(reply: Promise[Any], task : java.util.concurrent.Future[_])
