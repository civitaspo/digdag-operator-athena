package pro.civitaspo.digdag.plugin.athena.wrapper

import java.time.Duration
import java.util.concurrent.Callable

import io.digdag.util.RetryExecutor
import io.digdag.util.RetryExecutor.{GiveupAction, RetryAction, RetryGiveupException, RetryPredicate}

case class ParamInWrapper(timeoutDurationMillis: Int, totalWaitMillisCounter: Iterator[Int] = Stream.from(0).iterator, hasOnRetry: Boolean = false)
case class ParamInRetry(e: Exception, retryCount: Int, retryLimit: Int, retryWaitMillis: Int, totalWaitMillis: Long)
case class ParamInGiveup(firstException: Exception, lastException: Exception)

class RetryableException(message: String = "", cause: Throwable = null) extends RuntimeException(message, cause)
class NotRetryableException(message: String = "", cause: Throwable = null) extends RuntimeException(message, cause)

class RetryExecutorWrapper(exe: RetryExecutor, param: ParamInWrapper) {

  def withRetryLimit(count: Int): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withRetryLimit(count), param)
  }

  def withInitialRetryWait(duration: Duration): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withInitialRetryWait(duration.toMillis.toInt), param)
  }

  def withMaxRetryWait(duration: Duration): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withMaxRetryWait(duration.toMillis.toInt), param)
  }

  def withWaitGrowRate(rate: Double): RetryExecutorWrapper = {
    RetryExecutorWrapper(exe.withWaitGrowRate(rate), param)
  }

  def withTimeout(duration: Duration): RetryExecutorWrapper = {
    val newParam: ParamInWrapper = ParamInWrapper(duration.toMillis.toInt, param.totalWaitMillisCounter, param.hasOnRetry)
    RetryExecutorWrapper(exe, newParam)
  }

  def retryIf(retryable: Exception => Boolean): RetryExecutorWrapper = {
    val r = new RetryPredicate {
      override def test(t: Exception): Boolean = retryable(t)
    }
    RetryExecutorWrapper(exe.retryIf(r), param)
  }

  def onRetry(f: ParamInRetry => Unit = _ => ()): RetryExecutorWrapper = {
    val r = new RetryAction {
      override def onRetry(exception: Exception, retryCount: Int, retryLimit: Int, retryWait: Int): Unit = {
        val totalWaitMillis: Int = param.totalWaitMillisCounter.next()
        if (totalWaitMillis > param.timeoutDurationMillis) {
          throw new RetryGiveupException(new IllegalStateException(s"Total Wait: ${totalWaitMillis}ms is exceeded Timeout: ${param.timeoutDurationMillis}ms"))
        }
        (1 until retryWait).foreach(_ => param.totalWaitMillisCounter.next())
        f(ParamInRetry(exception, retryCount, retryLimit, retryWait, totalWaitMillis))
      }
    }
    val newParam: ParamInWrapper = ParamInWrapper(param.timeoutDurationMillis, param.totalWaitMillisCounter, true)
    RetryExecutorWrapper(exe.onRetry(r), newParam)
  }

  def onGiveup(f: ParamInGiveup => Unit): RetryExecutorWrapper = {
    val g = new GiveupAction {
      override def onGiveup(firstException: Exception, lastException: Exception): Unit = {
        f(ParamInGiveup(firstException, lastException))
      }
    }
    RetryExecutorWrapper(exe.onGiveup(g), param)
  }

  def runInterruptible[T](f: => T): T = {
    executeWithWrappedRetryExecutorWrapper { w =>
      w._runInterruptible(f)
    }
  }

  def run[T](f: => T): T = {
    executeWithWrappedRetryExecutorWrapper { w =>
      w._run(f)
    }
  }

  private def _runInterruptible[T](f: => T): T = {
    val c = new Callable[T] {
      override def call(): T = f
    }
    exe.runInterruptible(c)
  }

  private def _run[T](f: => T): T = {
    val c = new Callable[T] {
      override def call(): T = f
    }
    exe.run(c)
  }

  private def executeWithWrappedRetryExecutorWrapper[T](f: RetryExecutorWrapper => T): T = {
    val wrapped = if (!param.hasOnRetry) onRetry() else this
    try f(wrapped)
    catch {
      case ex: RetryGiveupException => throw new NotRetryableException(cause = ex)
    }
  }
}

object RetryExecutorWrapper {

  def apply(exe: RetryExecutor, param: ParamInWrapper): RetryExecutorWrapper = new RetryExecutorWrapper(exe, param)

  def apply(): RetryExecutorWrapper = RetryExecutorWrapper(RetryExecutor.retryExecutor(), ParamInWrapper(Int.MaxValue))

}
