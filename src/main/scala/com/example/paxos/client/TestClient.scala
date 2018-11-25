package com.example.paxos.client

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig}
import com.example.paxos.protocol.endpoint.PaxosSyncHello
import com.example.srpc.nettyrpc.{RpcEndpointRef, RpcEnv}

/**
  * Created by yilong on 2018/11/20.
  */
object TestClient {
  val defaultHost = "localhost"
  val defaultPort = 12201

  def main(args: Array[String]): Unit = {
    val host = args.length >= 1 match {
      case true => args(0)
      case _ => defaultHost
    }

    val port = args.length >= 2 match {
      case true => args(1).toInt
      case _ => defaultPort
    }

    val data = args.length >= 3 match {
      case true => args(2)
      case _ => System.currentTimeMillis().toString
    }

    processData(host, port, data.getBytes("utf-8"))
  }

  def processData(host : String, port : Int, data : Array[Byte]) = {
    val rpcEnv: RpcEnv = RpcEnv.create(new RpcConfig(), host, port, true)

    val ref1 = rpcEnv.setupEndpointRef(new HostPort(host, port), "syncer")

    var result = ref1.askSync[Any](PaxosSyncHello(0, new HostPort("localhost",0)), 1000*100)
    println(result)

    //val ref2 = rpcEnv.setupEndpointRef(new HostPort(host, port), "learner")
    //result = ref2.askSync[Any](PaxosChosen(PaxosData(0, 0, data)), 1000*100)
    //println(result)

    println("----------------------------- end --------------------------------")

    rpcEnv.shutdownNow()
  }
}
