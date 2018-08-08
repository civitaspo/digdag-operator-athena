package pro.civitaspo.digdag.plugin.athena.operator

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration

import com.amazonaws.services.athena.model.{
  GetQueryExecutionRequest,
  QueryExecution,
  QueryExecutionContext,
  QueryExecutionState,
  ResultConfiguration,
  StartQueryExecutionRequest
}
import com.amazonaws.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, QUEUED, RUNNING, SUCCEEDED}
import com.google.common.base.Optional
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.wrapper.{NotRetryableException, ParamInGiveup, ParamInRetry, RetryableException, RetryExecutorWrapper}

import scala.util.Try
import scala.util.hashing.MurmurHash3

class AthenaQueryOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  protected val queryOrFile: String = params.get("_command", classOf[String])
  protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena")
  protected val database: Optional[String] = params.getOptional("database", classOf[String])
  protected val output: String = params.get("output", classOf[String])
  protected val timeout: DurationParam = params.get("timeout", classOf[DurationParam], DurationParam.parse("10m"))

  protected lazy val query: String = {
    val t = Try {
      val f = workspace.getFile(queryOrFile)
      workspace.templateFile(templateEngine, f.getPath, UTF_8, params)
    }
    t.getOrElse(queryOrFile)
  }

  protected lazy val clientRequestToken: String = {
    val queryHash: Int = MurmurHash3.bytesHash(query.getBytes(UTF_8), 0)
    s"$tokenPrefix-$sessionUuid-$queryHash"
  }

  override def runTask(): TaskResult = {
    val execId: String = withAthena { athena =>
      val req = buildStartQueryExecutionRequest
      val r = athena.startQueryExecution(req)
      r.getQueryExecutionId
    }

    null
  }

  def buildStartQueryExecutionRequest: StartQueryExecutionRequest = {
    val req = new StartQueryExecutionRequest()

    req.setClientRequestToken(clientRequestToken)
    if (database.isPresent) req.setQueryExecutionContext(new QueryExecutionContext().withDatabase(database.get()))
    req.setQueryString(query)
    req.setResultConfiguration(new ResultConfiguration().withOutputLocation(output))

    req
  }

  def pollingQueryExecution(execId: String): QueryExecution = {
    val req = new GetQueryExecutionRequest().withQueryExecutionId(execId)

    RetryExecutorWrapper()
      .withInitialRetryWait(Duration.ofSeconds(1L)) // TODO: make it configurable?
      .withMaxRetryWait(Duration.ofSeconds(30L)) // TODO: make it configurable?
      .withRetryLimit(Integer.MAX_VALUE)
      .withWaitGrowRate(1.1) // TODO: make it configurable?
      .withTimeout(timeout.getDuration)
      .retryIf {
        case _: RetryableException => true
        case _ => false
      }
      .onRetry { p: ParamInRetry =>
        logger.info(s"[$operatorName] polling ${p.e.getMessage} (next: ${p.retryCount}, total wait: ${p.totalWaitMillis} ms)")
      }
      .onGiveup { p: ParamInGiveup =>
        logger.error(
          s"[$operatorName] failed to execute query `$execId`. You can see last exception's stacktrace. (first exception message: ${p.firstException.getMessage}, last exception message: ${p.lastException.getMessage})",
          p.lastException
        )
      }
      .runInterruptible {
        val r = withAthena(_.getQueryExecution(req))
        val qe = r.getQueryExecution
        val status = Option(qe.getStatus).getOrElse(throw new RetryableException("status is null"))
        val stateStr = Option(status.getState).getOrElse(throw new RetryableException("state is null"))

        QueryExecutionState.fromValue(stateStr) match {
          case SUCCEEDED => qe
          case FAILED => throw new NotRetryableException(s"[$operatorName] query is `$FAILED`")
          case CANCELLED => throw new NotRetryableException(s"[$operatorName] query is `$CANCELLED`")
          case RUNNING => throw new RetryableException(s"query is `$RUNNING`")
          case QUEUED => throw new RetryableException(s"query is `$QUEUED`")
        }
      }
  }
}
