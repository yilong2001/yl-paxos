package com.example.paxos.util

import scala.collection.mutable

/**
  * Created by yilong on 2018/11/18.
  */
class PaxosConfig {
  private val configMap = new mutable.HashMap[String, Any]()
  def set(key:String, value:Any): Unit = {
    configMap.put(key,value)
  }
  def get(key:String) : Any = {
    configMap.get(key).get
  }
}
