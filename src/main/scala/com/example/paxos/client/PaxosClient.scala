package com.example.paxos.client

import com.example.jrpc.nettyrpc.rpc.{HostPort, RpcConfig}
import com.example.srpc.nettyrpc.{RpcEndpointRef, RpcEnv}
import org.apache.commons.logging.LogFactory

/**
  * Created by yilong on 2018/11/19.
  */
class PaxosClient {

}

object PaxosClient extends App {
  private val log = LogFactory.getLog(classOf[PaxosClient])

  val defaultHost = "localhost"
  val defaultPort = 12201

  def doMain(args: Array[String]): Unit = {
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
      case _ => System.currentTimeMillis().toString + port
    }

    processData(host, port, data.getBytes("utf-8"))
  }

  def processData(host : String, port : Int, data : Array[Byte]) = {
    val rpcConfig = new RpcConfig
    rpcConfig.setAskTimeoutMs(3000)
    val rpcEnv: RpcEnv = RpcEnv.create(rpcConfig, host, port, true)

    val epRef: RpcEndpointRef = rpcEnv.setupEndpointRef(new HostPort(host, port), "proposer")

    try {
      var result = epRef.askSync[Any](PaxosClientReq(data), 3000 * 1)
      result match {
        case PaxosClientReply(rd, bts) => log.info(s" ******************  reply is success :  ${rd}, ${new String(bts, "utf-8")} ****************** ")
        case _ => log.info(s" ******************  reply is bad :  ${result} ****************** ")
      }
    } catch {
      case e : Exception =>e.printStackTrace(); log.error(s"************************ exception : ${e} **************** ")
    } finally {
      rpcEnv.shutdownNow()
      log.info("------------------------------ over --------------------------------")
    }
  }

  doMain(this.args)
}