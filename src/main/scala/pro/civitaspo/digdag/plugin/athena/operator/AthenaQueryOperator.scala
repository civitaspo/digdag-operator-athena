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
import com.amazonaws.services.s3.AmazonS3URI
import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigException, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.wrapper.{NotRetryableException, ParamInGiveup, ParamInRetry, RetryableException, RetryExecutorWrapper}

import scala.collection.JavaConverters._
import scala.util.Try
import scala.util.hashing.MurmurHash3

class AthenaQueryOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  protected val queryOrFile: String = params.get("_command", classOf[String])
  protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena")
  protected val database: Optional[String] = params.getOptional("database", classOf[String])
  protected val output: String = {
    val o = params.get("output", classOf[String])
    if (o.endsWith("/")) o else s"$o/"
  }
  protected val keepMetadata: Boolean = params.get("keep_metadata", classOf[Boolean], false)
  protected val saveMode: String = params.get("save_mode", classOf[String], "overwrite")
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
    saveMode match {
      case "error_if_exists" =>
        val uri = new AmazonS3URI(output, false)
        val result = withS3(_.listObjectsV2(uri.getBucket, uri.getKey))

        if (!result.getObjectSummaries.isEmpty) throw new IllegalStateException(s"[$operatorName] some objects already exists in `$output`.")
        val execId: String = startQueryExecution
        val qe: QueryExecution = pollingQueryExecution(execId)

        if (!keepMetadata) {
          val metadataLocation: String = s"${qe.getResultConfiguration.getOutputLocation}.metadata"
          logger.info(s"[$operatorName] Delete `$metadataLocation`.")
          val mUri = new AmazonS3URI(metadataLocation, false)
          withS3(_.deleteObject(mUri.getBucket, mUri.getKey))
        }
        val p: Config = buildStoredParamFromQueryExecution(qe)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
        builder.storeParams(p)
        builder.build()
      case "ignore" =>
        val uri = new AmazonS3URI(output, false)
        val result = withS3(_.listObjectsV2(uri.getBucket, uri.getKey))

        if (!result.getObjectSummaries.isEmpty) {
          logger.info(s"[$operatorName] some objects already exists in $output so do nothing in this session: `$sessionUuid`.")
          val builder = TaskResult.defaultBuilder(request)
          builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
          return builder.build()
        }
        val execId: String = startQueryExecution
        val qe: QueryExecution = pollingQueryExecution(execId)

        if (!keepMetadata) {
          val metadataLocation: String = s"${qe.getResultConfiguration.getOutputLocation}.metadata"
          logger.info(s"[$operatorName] Delete $metadataLocation.")
          val mUri = new AmazonS3URI(metadataLocation, false)
          withS3(_.deleteObject(mUri.getBucket, mUri.getKey))
        }

        val p: Config = buildStoredParamFromQueryExecution(qe)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
        builder.storeParams(p)
        builder.build()
      case "append" =>
        val execId: String = startQueryExecution
        val qe: QueryExecution = pollingQueryExecution(execId)

        if (!keepMetadata) {
          val metadataLocation: String = s"${qe.getResultConfiguration.getOutputLocation}.metadata"
          logger.info(s"[$operatorName] Delete $metadataLocation.")
          val mUri = new AmazonS3URI(metadataLocation, false)
          withS3(_.deleteObject(mUri.getBucket, mUri.getKey))
        }

        val p: Config = buildStoredParamFromQueryExecution(qe)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
        builder.storeParams(p)
        builder.build()
      case "overwrite" =>
        val uri = new AmazonS3URI(output, false)
        val result = withS3(_.listObjectsV2(uri.getBucket, uri.getKey))

        val execId: String = startQueryExecution
        val qe: QueryExecution = pollingQueryExecution(execId)
        result.getObjectSummaries.asScala.foreach { summary =>
          logger.info(s"[$operatorName] Delete s3://${summary.getBucketName}/${summary.getKey}")
          withS3(_.deleteObject(summary.getBucketName, summary.getKey))
        }
        if (!keepMetadata) {
          val metadataLocation: String = s"${qe.getResultConfiguration.getOutputLocation}.metadata"
          logger.info(s"[$operatorName] Delete $metadataLocation.")
          val mUri = new AmazonS3URI(metadataLocation, false)
          withS3(_.deleteObject(mUri.getBucket, mUri.getKey))
        }

        val p: Config = buildStoredParamFromQueryExecution(qe)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
        builder.storeParams(p)
        builder.build()
      case unknown => throw new ConfigException(s"[$operatorName] Save mode '$unknown' is unsupported.")
    }
  }

  def startQueryExecution: String = {
    val req = buildStartQueryExecutionRequest
    val r = withAthena(_.startQueryExecution(req))
    r.getQueryExecutionId
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
          case SUCCEEDED =>
            logger.info(s"[$operatorName] query is `$SUCCEEDED`")
            qe
          case FAILED => throw new NotRetryableException(message = s"[$operatorName] query is `$FAILED`")
          case CANCELLED => throw new NotRetryableException(message = s"[$operatorName] query is `$CANCELLED`")
          case RUNNING => throw new RetryableException(message = s"query is `$RUNNING`")
          case QUEUED => throw new RetryableException(message = s"query is `$QUEUED`")
        }
      }
  }

  def buildStoredParamFromQueryExecution(qe: QueryExecution): Config = {
    val ret = cf.create()
    val lastQueryParam = ret.getNestedOrSetEmpty("athena").getNestedOrSetEmpty("last_query")

    lastQueryParam.set("id", qe.getQueryExecutionId)
    lastQueryParam.set("database", Try(Option(qe.getQueryExecutionContext.getDatabase)).getOrElse(None).getOrElse(Optional.absent()))
    lastQueryParam.set("query", qe.getQuery)
    lastQueryParam.set("output", qe.getResultConfiguration.getOutputLocation)
    lastQueryParam.set("scan_bytes", Try(Option(qe.getStatistics.getDataScannedInBytes)).getOrElse(None).getOrElse(Optional.absent()))
    lastQueryParam.set("exec_millis", Try(Option(qe.getStatistics.getEngineExecutionTimeInMillis)).getOrElse(None).getOrElse(Optional.absent()))
    lastQueryParam.set("state", Try(Option(qe.getStatus.getState)).getOrElse(None).getOrElse(Optional.absent()))
    lastQueryParam.set("state_change_reason", Try(Option(qe.getStatus.getStateChangeReason)).getOrElse(None).getOrElse(Optional.absent()))
    lastQueryParam.set("submitted_at", Try(Option(qe.getStatus.getSubmissionDateTime.getTime / 1000)).getOrElse(None).getOrElse(Optional.absent()))
    lastQueryParam.set("completed_at", Try(Option(qe.getStatus.getCompletionDateTime.getTime / 1000)).getOrElse(None).getOrElse(Optional.absent()))

    ret
  }
}
