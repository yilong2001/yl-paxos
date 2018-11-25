package com.example.paxos.io
import java.io.{File, FileOutputStream, IOException}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.example.paxos.exception.{PaxosIOException, PaxosRoundWrongException}
import com.example.paxos.instance.PaxosInstance

import scala.collection.mutable.ArrayBuffer
import java.io.File
import java.nio.file.Files

import com.example.paxos.protocol.endpoint.PaxosData
import com.example.paxos.util.PaxosUtils
import org.apache.commons.logging.LogFactory

import scala.io.Source

/**
  * Created by yilong on 2018/11/18.
  */
class DefaultStore(instance : PaxosInstance) extends Store {
  private val log = LogFactory.getLog(classOf[DefaultStore])

  //TODO:
  val memStoreMap = new ConcurrentHashMap[Long, PaxosData]()
  val lastRound = new AtomicLong(-1)

  def recursiveListFiles(f: File): Array[File] = {
    val these = f.listFiles
    if (these == null) {
      Array()
    } else {
      these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
    }
  }

  override def start(): Unit = {
    //TODO: only for example, load all data
    log.info(s"start ${instance.getBaseDir()}")
    val f = new File(instance.getBaseDir())
    if (f != null) {
      val files = recursiveListFiles(f)
      var maxRd: Long = -1
      val fs = files.filter(f => !f.isDirectory).filter(f => f.getAbsolutePath.endsWith(".dat")).foreach(f => {
        val rd = (f.getName.split("\\.")).apply(0).toLong
        val v = readFile(f)
        if (maxRd < rd) {
          maxRd = rd
        }

        memStoreMap.put(rd, PaxosData(rd, -1, v))
      })

      lastRound.set(maxRd)
    }
  }

  @throws(classOf[Exception])
  override def write(data: PaxosData): Unit = {
    log.info(s"start write data : ${data.round},${data.posId},${PaxosUtils.getString(data.value)}")

    if (data.round != lastRound.get() + 1) {
      log.error(s"write round is not euqal last round+1:${data.round},${lastRound.get()},${new String(data.value, "utf-8")}")
      throw new PaxosRoundWrongException("write round is not euqal last round + 1")
    }

    val filename = buildFilename(data)
    try {
      writeFile(filename, data.value)
      val d = memStoreMap.putIfAbsent(data.round, data)
      if (d != null) {
        //TODO:
      } else {
        val ok = lastRound.compareAndSet(data.round-1, data.round)
        if (!ok) log.error(s"update last round failed, ${data.round}")
      }
    } catch {
      case e : Exception => {throw e}
    } finally {

    }
  }

  @throws(classOf[Exception])
  override def write(datas: List[PaxosData]): Unit = {
    datas.sortWith((d1,d2)=> d1.round<d2.round)

    log.info(s"start write datas : ${datas.size}, ${datas(0)}, ${datas(datas.size - 1)}")

    try {
      datas.foreach(d => write(d))
    } catch {
      case e:Exception => throw e
    } finally {
      //
    }
  }

  override def read(round: Long): PaxosData = {
    memStoreMap.get(round)
  }

  override def read(start: Long, end: Long): List[PaxosData] = {
    (start < end) match {
      case false => List()
      case _ => (start to end).map(rd => memStoreMap.get(rd)).toList
    }
  }

  override def getLastRound(): Long = {
    lastRound.get()
  }

  def formatLong(v : Long) : String = {
    val o = ("00000000000000000000"+v.toString)
    o.substring(o.size-20, o.size)
  }

  def buildFilename(data: PaxosData) : String = {
    instance.getBaseDir() + "/" +formatLong(data.round) + ".dat"
  }

  def setLastRound(newRd : Long) : Unit = {
    var stop = false
    while(!stop) {
      val rd = lastRound.get()
      //TODO: round 连续性？
      if (rd+1 != newRd) {
        stop = true
      } else {
        stop = lastRound.compareAndSet(rd, newRd)
      }
    }
  }

  @throws(classOf[PaxosIOException])
  def writeFile(filename : String, bytes : Array[Byte]) = {
    val f = new File(filename)
    if (!f.getParentFile.exists()) {
      f.getParentFile.mkdirs
    }
    f.createNewFile()

    val writer = new FileOutputStream(filename)

    try {
      writer.write(bytes)
    } catch {
      case e : Exception => throw new PaxosIOException("write "+filename+" failed : " + e.getMessage)
    } finally {
      writer.getFD.sync()
      writer.close()
    }

    println("writeFile -> " + filename)
  }

  @throws(classOf[PaxosIOException])
  def readFile(file : File) : Array[Byte] = {
    println("readFile -> " + file.getAbsolutePath)

    try {
      Files.readAllBytes(file.toPath)
    } catch {
      case e : Exception => e.printStackTrace(); throw new PaxosIOException("read "+file.getAbsolutePath+" failed : " + e.getMessage)
    } finally {

    }
  }
}
