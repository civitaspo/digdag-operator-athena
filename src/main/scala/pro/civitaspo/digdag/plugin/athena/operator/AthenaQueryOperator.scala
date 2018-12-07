package pro.civitaspo.digdag.plugin.athena.operator

import java.nio.charset.StandardCharsets.UTF_8
import java.time.Duration

import com.amazonaws.regions.{DefaultAwsRegionProviderChain, Regions}
import com.amazonaws.services.athena.model.{
  GetQueryExecutionRequest,
  QueryExecution,
  QueryExecutionContext,
  QueryExecutionState,
  ResultConfiguration,
  StartQueryExecutionRequest
}
import com.amazonaws.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, QUEUED, RUNNING, SUCCEEDED}
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest
import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.wrapper.{NotRetryableException, ParamInGiveup, ParamInRetry, RetryableException, RetryExecutorWrapper}

import scala.util.{Random, Try}
import scala.util.hashing.MurmurHash3

class AthenaQueryOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  object AmazonS3URI {
    def apply(path: String): AmazonS3URI = new AmazonS3URI(path, false)
  }

  case class LastQuery(
    id: String,
    database: Option[String] = None,
    query: String,
    output: AmazonS3URI,
    scanBytes: Option[Long] = None,
    execMillis: Option[Long] = None,
    state: QueryExecutionState,
    stateChangeReason: Option[String] = None,
    submittedAt: Option[Long] = None,
    completedAt: Option[Long] = None
  )

  object LastQuery {

    def apply(qe: QueryExecution): LastQuery = {
      new LastQuery(
        id = qe.getQueryExecutionId,
        database = Try(Option(qe.getQueryExecutionContext.getDatabase)).getOrElse(None),
        query = qe.getQuery,
        output = AmazonS3URI(qe.getResultConfiguration.getOutputLocation),
        scanBytes = Try(Option(qe.getStatistics.getDataScannedInBytes.toLong)).getOrElse(None),
        execMillis = Try(Option(qe.getStatistics.getEngineExecutionTimeInMillis.toLong)).getOrElse(None),
        state = QueryExecutionState.fromValue(qe.getStatus.getState),
        stateChangeReason = Try(Option(qe.getStatus.getStateChangeReason)).getOrElse(None),
        submittedAt = Try(Option(qe.getStatus.getSubmissionDateTime.getTime / 1000)).getOrElse(None),
        completedAt = Try(Option(qe.getStatus.getCompletionDateTime.getTime / 1000)).getOrElse(None)
      )
    }
  }

  protected val queryOrFile: String = params.get("_command", classOf[String])
  protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena")
  protected val database: Optional[String] = params.getOptional("database", classOf[String])
  @deprecated protected val outputOptional: Optional[String] = params.getOptional("output", classOf[String])
  protected val timeout: DurationParam = params.get("timeout", classOf[DurationParam], DurationParam.parse("10m"))
  protected val preview: Boolean = params.get("preview", classOf[Boolean], true)

  protected lazy val query: String = {
    val t = Try {
      if (queryOrFile.startsWith("s3://")) {
        val uri = AmazonS3URI(queryOrFile)
        val content = withS3(_.getObjectAsString(uri.getBucket, uri.getKey))
        templateEngine.template(content, params)
      }
      else {
        val f = workspace.getFile(queryOrFile)
        workspace.templateFile(templateEngine, f.getPath, UTF_8, params)
      }
    }
    t.getOrElse(queryOrFile)
  }

  protected lazy val clientRequestToken: String = {
    val queryHash: Int = MurmurHash3.bytesHash(query.getBytes(UTF_8), 0)
    val random: String = Random.alphanumeric.take(5).mkString
    s"$tokenPrefix-$sessionUuid-$queryHash-$random"
  }

  protected lazy val output: AmazonS3URI = {
    AmazonS3URI {
      if (outputOptional.isPresent) outputOptional.get()
      else {
        val accountId: String = withSts(_.getCallerIdentity(new GetCallerIdentityRequest())).getAccount
        val r = region.or(Try(new DefaultAwsRegionProviderChain().getRegion).getOrElse(Regions.DEFAULT_REGION.getName))
        s"s3://aws-athena-query-results-$accountId-$r"
      }
    }
  }

  @deprecated private def showMessageIfUnsupportedOptionExists(): Unit = {
    if (params.getOptional("keep_metadata", classOf[Boolean]).isPresent) {
      logger.warn("`keep_metadata` option has removed, and the behaviour is the same as `keep_metadata: true`.")
    }
    if (params.getOptional("save_mode", classOf[String]).isPresent) {
      logger.warn("`save_mode` option has removed, and the behaviour is the same as `save_mode: append` .")
    }
    if (outputOptional.isPresent) {
      logger.warn("`output` option will be removed, and the current default value will be always used.")
    }
  }

  override def runTask(): TaskResult = {
    showMessageIfUnsupportedOptionExists()

    val execId: String = startQueryExecution
    val lastQuery: LastQuery = pollingQueryExecution(execId)

    logger.info(s"[$operatorName] Executed ${lastQuery.id} (scan: ${lastQuery.scanBytes.orNull} bytes, time: ${lastQuery.execMillis.orNull}ms)")
    val p: Config = buildLastQueryParam(lastQuery)

    val builder = TaskResult.defaultBuilder(request)
    builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
    builder.storeParams(p)
    if (preview) builder.subtaskConfig(buildPreviewSubTaskConfig(lastQuery))
    builder.build()
  }

  protected def startQueryExecution: String = {
    val req = buildStartQueryExecutionRequest
    val r = withAthena(_.startQueryExecution(req))
    r.getQueryExecutionId
  }

  protected def buildStartQueryExecutionRequest: StartQueryExecutionRequest = {
    val req = new StartQueryExecutionRequest()

    req.setClientRequestToken(clientRequestToken)
    if (database.isPresent) req.setQueryExecutionContext(new QueryExecutionContext().withDatabase(database.get()))
    req.setQueryString(query)
    req.setResultConfiguration(new ResultConfiguration().withOutputLocation(output.toString))

    req
  }

  protected def pollingQueryExecution(execId: String): LastQuery = {
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
        val lastQuery = LastQuery(r.getQueryExecution)

        lastQuery.state match {
          case SUCCEEDED =>
            logger.info(s"[$operatorName] query is `$SUCCEEDED`")
            lastQuery
          case FAILED => throw new NotRetryableException(message = s"[$operatorName] query is `$FAILED`")
          case CANCELLED => throw new NotRetryableException(message = s"[$operatorName] query is `$CANCELLED`")
          case RUNNING => throw new RetryableException(message = s"query is `$RUNNING`")
          case QUEUED => throw new RetryableException(message = s"query is `$QUEUED`")
        }
      }
  }

  protected def buildLastQueryParam(lastQuery: LastQuery): Config = {
    val ret = cf.create()
    val lastQueryParam = ret.getNestedOrSetEmpty("athena").getNestedOrSetEmpty("last_query")

    lastQueryParam.set("id", lastQuery.id)
    lastQueryParam.set("database", lastQuery.database.getOrElse(Optional.absent()))
    lastQueryParam.set("query", lastQuery.query)
    lastQueryParam.set("output", lastQuery.output.toString) // TODO: It's not always csv, so should change.
    lastQueryParam.set("scan_bytes", lastQuery.scanBytes.getOrElse(Optional.absent()))
    lastQueryParam.set("exec_millis", lastQuery.execMillis.getOrElse(Optional.absent()))
    lastQueryParam.set("state", lastQuery.state)
    lastQueryParam.set("state_change_reason", lastQuery.stateChangeReason.getOrElse(Optional.absent()))
    lastQueryParam.set("submitted_at", lastQuery.submittedAt.getOrElse(Optional.absent()))
    lastQueryParam.set("completed_at", lastQuery.completedAt.getOrElse(Optional.absent()))

    ret
  }

  protected def buildPreviewSubTaskConfig(lastQuery: LastQuery): Config = {
    val subTask: Config = cf.create()
    subTask.set("_type", "athena.preview")
    subTask.set("_command", lastQuery.id)
    subTask.set("max_rows", 10)

    subTask.set("auth_method", authMethod)
    subTask.set("profile_name", profileName)
    if (profileFile.isPresent) subTask.set("profile_file", profileFile.get())
    subTask.set("use_http_proxy", useHttpProxy)
    if (region.isPresent) subTask.set("region", region.get())
    if (endpoint.isPresent) subTask.set("endpoint", endpoint.get())

    subTask
  }
}
