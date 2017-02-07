package com.kakao.cuesheet.convert

import java.util.concurrent._

import com.kakao.mango.concurrent.NamedExecutors
import com.google.common.util.concurrent.MoreExecutors

trait ExecutorSupplier extends Serializable {
  def get(): ExecutorService
}

object ExecutorSupplier {

  val runNow = new ExecutorSupplier {
    override def get(): ExecutorService = MoreExecutors.sameThreadExecutor()
  }

  def cached(name: String = "cached-executor-supplier", daemon: Boolean = true) = new ExecutorSupplier {
    override def get(): ExecutorService = NamedExecutors.cached(name, daemon)
  }

  def fixed(size: Int, name: String = "fixed-executor-supplier", daemon: Boolean = true) = new ExecutorSupplier {
    override def get(): ExecutorService = NamedExecutors.fixed(name, size, daemon)
  }

}
