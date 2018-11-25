package com.example.paxos.protocol.exector

/**
  * Created by yilong on 2018/11/22.
  */
object CompletionStatus extends Enumeration {
  type CompletionStatus = Value
  val Success = Value("success")
  val Failed = Value("failed")
  val Skipped = Value("skipped")
  val SyncPending = Value("syncPending")
}
