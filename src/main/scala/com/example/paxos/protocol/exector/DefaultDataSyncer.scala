package com.example.paxos.protocol.exector

import java.util.concurrent.{Callable, TimeUnit}

import com.example.jrpc.nettyrpc.rpc.HostPort
import com.example.paxos.exception.PaxosException
import com.example.paxos.instance.PaxosInstance
import com.example.paxos.protocol.endpoint.{PaxosData, PaxosSyncHello, PaxosSyncReply, PaxosSyncReq}
import com.example.paxos.protocol.role.DataSyncer
import com.example.paxos.protocol.util.ThreadServiceExecutor
import com.example.srpc.nettyrpc.util.ThreadUtils
import org.apache.commons.logging.LogFactory

import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
  * Created by yilong on 2018/11/17.
  */
class DefaultDataSyncer(val instance : PaxosInstance,
                        val directPeerNum : Int) extends DataSyncer {
  private val log = LogFactory.getLog(classOf[DefaultDataSyncer])

  val sorted = instance.getAllAddrs().sortWith((ref1, ref2) => (ref1.hostPort().compareTo(ref2.hostPort()) < 0))

  var localid = sorted.indexWhere(hp => hp.hostPort().equals(instance.getLocalAddr().hostPort()))

  val peers = (1 to directPeerNum).map(x=>(localid+x) % sorted.size).map(x=>sorted.apply(x))

  var timerSchd : java.util.concurrent.ScheduledFuture[_] = null

  override def onSayHello(round: Long, hostPort: HostPort): Any = {
    log.info(s"onSayHello : round=${round}, src.hp=${hostPort}, dest.hp=${instance.getLocalAddr()}")
    PaxosSyncHello(instance.getCurrentRound(), instance.getLocalAddr())
  }

  override def onSyncReq(start: Long, end: Long): Any = {
    log.info(s"onSyncReq : start=${start}, end=${end}")
    val list = if (start > end || end >= instance.getCurrentRound()) {
      List()
    } else {
      (start to end).map(rd => instance.getStore().read(rd)).toList
    }

    PaxosSyncReply(list)
  }

  def sayHello(myRound : Long, myHp : HostPort): (Long, HostPort) = {
    val latch = new java.util.concurrent.CountDownLatch(peers.size)

    val result = peers.map(hp => instance.getDataSyncerRpcEndpointRef(hp)).map(peer => {
      val f = ThreadServiceExecutor[Any]({
        try {
          val rsp = peer.askSync[Any](PaxosSyncHello(instance.getCurrentRound(), instance.getLocalAddr()), instance.getBaseTimeoutInterval())
          rsp match {
            case PaxosSyncHello(rd, hp) => (rsp)
            case _ => log.error(s"say hello reply not ok ${rsp}"); throw new PaxosException("say hello reply not ok")
          }
        } catch {
          case e : Exception => { log.error(e.getMessage); throw new PaxosException(e.getMessage) }
        }
      })(ThreadServiceExecutor.execContext)

      f.onComplete(info => {
        latch.countDown()
      })(ThreadServiceExecutor.execContext)

      f
    })

    //TODO: ???
    val isOk = latch.await(instance.getBaseTimeoutInterval()*2, java.util.concurrent.TimeUnit.MILLISECONDS)

    var maxround : Long = -1
    var hostPort : HostPort = null
    result.foreach(f => {
      val r = f.value.getOrElse(new PaxosException("unknown error")) match {
        case Success(PaxosSyncHello(rd,hp)) => if (maxround < rd) {maxround = rd; hostPort=hp}
      }
    })

    log.info(s"paxos PaxosSyncHello over : ${maxround}, ${hostPort}")

    //success or failed
    (maxround,hostPort)
  }

  def scheduleSayHello() : java.util.concurrent.ScheduledFuture[_] = {
    val run = new Runnable {
      override def run(): Unit = {
        try {
          val curRd = instance.getCurrentRound()
          val (round, hostport) = sayHello(curRd, instance.getLocalAddr())
          if (round > curRd) {
            performSync(curRd, round - 1, hostport)
          }
        } catch {
          case e : Exception => log.error(e)
        }
      }
    }

    ThreadServiceExecutor.syncTimerSchd.scheduleWithFixedDelay(run, 1000*60, 1000*5, TimeUnit.MILLISECONDS)
  }

  def performSync(start:Long, end:Long, hostPort: HostPort) : Unit = {
    if (end < start) {
      //TODO:???
    }

    val f = ThreadServiceExecutor[Any]({
      try {
        //val rsp =
        instance.getDataSyncerRpcEndpointRef(hostPort).askSync[Any](PaxosSyncReq(start, end), instance.getBaseTimeoutInterval()*(end - start))
        //rsp match {
          //case PaxosSyncReply(datas) => rsp
          //case _ => println(rsp.toString); (new PaxosException("sync reply not ok"))
        //}
      } catch {
        case e : Exception => { e.printStackTrace(); throw new PaxosException(e.getMessage) }
      }
    })(ThreadServiceExecutor.execContext)

    ThreadUtils.awaitResult[Any](f,
      Duration(instance.getBaseTimeoutInterval()*(end - start),TimeUnit.MILLISECONDS))

    f.value.getOrElse(new PaxosException("paxos sync reply error")) match {
      case Success(PaxosSyncReply(datas)) => {
        updateSyncData(datas)
      }
    }
  }

  def updateSyncData(datas : List[PaxosData]) : Unit = {
    instance.getStore().write(datas)
  }

  override def start(): Unit = {
    timerSchd = scheduleSayHello()
  }
  override def stop(): Unit = {
    if (timerSchd != null) timerSchd.cancel(true)
  }

}
