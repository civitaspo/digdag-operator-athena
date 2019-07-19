package pro.civitaspo.digdag.plugin.athena.aws.athena


import java.util.concurrent.{Executors, ExecutorService}

import com.amazonaws.services.athena.model.{GetQueryExecutionRequest, GetQueryExecutionResult, QueryExecutionState}
import com.amazonaws.waiters.{PollingStrategy, PollingStrategyContext, SdkFunction, Waiter, WaiterAcceptor, WaiterBuilder, WaiterParameters, WaiterState}
import com.amazonaws.waiters.PollingStrategy.{DelayStrategy, RetryStrategy}
import com.typesafe.scalalogging.LazyLogging
import io.digdag.util.DurationParam


case class AthenaQueryWaiter(athena: Athena,
                             successStats: Seq[QueryExecutionState],
                             failureStats: Seq[QueryExecutionState],
                             executorService: ExecutorService = Executors.newFixedThreadPool(50),
                             timeout: DurationParam)
    extends LazyLogging
{
    def wait(executionId: String): Unit =
    {
        newWaiter().run(newWaiterParameters(executionId = executionId))
    }

    private def newWaiterParameters(executionId: String): WaiterParameters[GetQueryExecutionRequest] =
    {
        new WaiterParameters[GetQueryExecutionRequest](new GetQueryExecutionRequest().withQueryExecutionId(executionId))
    }

    private def newWaiter(): Waiter[GetQueryExecutionRequest] =
    {
        new WaiterBuilder[GetQueryExecutionRequest, GetQueryExecutionResult]()
            .withSdkFunction(newWaiterFunction())
            .withExecutorService(executorService)
            .withDefaultPollingStrategy(newPollingStrategy())
            .withAcceptors(
                newStatusLoggerAcceptor(),
                newSuccessAcceptor(),
                newFailureAcceptor()
                )
            .build()
    }

    private def newWaiterFunction(): SdkFunction[GetQueryExecutionRequest, GetQueryExecutionResult] =
    {
        input: GetQueryExecutionRequest => {
            athena.withAthena(_.getQueryExecution(input))
        }
    }

    private def newPollingStrategy(): PollingStrategy =
    {
        new PollingStrategy(newTimeoutRetryStrategy(), newFullJitterDelayStrategy())
    }

    private def newTimeoutRetryStrategy(): RetryStrategy =
    {
        new RetryStrategy
        {
            val startedAt: Long = System.currentTimeMillis()

            override def shouldRetry(pollingStrategyContext: PollingStrategyContext): Boolean =
            {
                (System.currentTimeMillis() - startedAt) < timeout.getDuration.toMillis
            }
        }
    }

    private def newFullJitterDelayStrategy(): DelayStrategy =
    {
        // ref. https://aws.amazon.com/jp/blogs/architecture/exponential-backoff-and-jitter/
        new DelayStrategy
        {
            // TODO: make this configurable?
            val baseSeconds: Double = 1.0
            val capSeconds: Double = 30.0

            override def delayBeforeNextRetry(pollingStrategyContext: PollingStrategyContext): Unit =
            {
                val numAttempts = pollingStrategyContext.getRetriesAttempted
                val sleepSeconds = math.random() * math.min(capSeconds, baseSeconds * math.pow(2.0, numAttempts))
                Thread.sleep((sleepSeconds * 1000).toLong)
            }
        }
    }

    private def newStatusLoggerAcceptor(): WaiterAcceptor[GetQueryExecutionResult] =
    {
        new WaiterAcceptor[GetQueryExecutionResult]
        {
            override def getState: WaiterState =
            {
                throw new IllegalStateException("This Acceptor is only for logging query execution status, so cannot return the waiter state.")
            }

            override def matches(output: GetQueryExecutionResult): Boolean =
            {
                // NOTE: Query string is noisy for logging, so suppress the query string logging.
                logger.info(s"Waiting the query: ${output.getQueryExecution.clone().withQuery(null).toString}")

                // NOTE: This is because this Acceptor is only for logging query execution status.
                false
            }
        }
    }

    private def newSuccessAcceptor(): WaiterAcceptor[GetQueryExecutionResult] =
    {
        new WaiterAcceptor[GetQueryExecutionResult]
        {
            override def getState: WaiterState =
            {
                WaiterState.SUCCESS
            }

            override def matches(output: GetQueryExecutionResult): Boolean =
            {
                val state: QueryExecutionState = QueryExecutionState.fromValue(output.getQueryExecution.getStatus.getState)
                successStats.contains(state)
            }
        }
    }

    private def newFailureAcceptor(): WaiterAcceptor[GetQueryExecutionResult] =
    {
        new WaiterAcceptor[GetQueryExecutionResult]
        {
            override def getState: WaiterState =
            {
                WaiterState.FAILURE
            }

            override def matches(output: GetQueryExecutionResult): Boolean =
            {
                val state: QueryExecutionState = QueryExecutionState.fromValue(output.getQueryExecution.getStatus.getState)
                failureStats.contains(state)
            }
        }
    }
}
