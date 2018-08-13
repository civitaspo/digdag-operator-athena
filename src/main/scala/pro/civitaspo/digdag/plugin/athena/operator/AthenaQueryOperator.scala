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
import com.amazonaws.services.s3.model.S3ObjectSummary
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

  object AmazonS3URI {
    def apply(path: String): AmazonS3URI = new AmazonS3URI(path, false)
  }

  sealed abstract class SaveMode

  object SaveMode {
    final case object Append extends SaveMode
    final case object ErrorIfExists extends SaveMode
    final case object Ignore extends SaveMode
    final case object Overwrite extends SaveMode

    def apply(mode: String): SaveMode = {
      mode match {
        case "append" => Append
        case "error_if_exists" => ErrorIfExists
        case "ignore" => Ignore
        case "overwrite" => Overwrite
        case unknown => throw new ConfigException(s"[$operatorName] save mode '$unknown' is unsupported.")
      }
    }
  }

  case class LastQuery(
    id: String,
    database: Option[String] = None,
    query: String,
    outputCsvUri: AmazonS3URI,
    outputCsvMetadataUri: AmazonS3URI,
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
        outputCsvUri = AmazonS3URI(qe.getResultConfiguration.getOutputLocation),
        outputCsvMetadataUri = AmazonS3URI(s"${qe.getResultConfiguration.getOutputLocation}.metadata"),
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
  protected val output: AmazonS3URI = {
    val o = params.get("output", classOf[String])
    AmazonS3URI(if (o.endsWith("/")) o else s"$o/")
  }
  protected val keepMetadata: Boolean = params.get("keep_metadata", classOf[Boolean], false)
  protected val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "overwrite"))
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
      case SaveMode.ErrorIfExists =>
        val result = withS3(_.listObjectsV2(output.getBucket, output.getKey))
        if (!result.getObjectSummaries.isEmpty) throw new IllegalStateException(s"[$operatorName] some objects already exists in `$output`.")
      case SaveMode.Ignore =>
        val result = withS3(_.listObjectsV2(output.getBucket, output.getKey))
        if (!result.getObjectSummaries.isEmpty) {
          logger.info(s"[$operatorName] some objects already exists in $output so do nothing in this session: `$sessionUuid`.")
          val builder = TaskResult.defaultBuilder(request)
          builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
          return builder.build()
        }
      case _ => // do nothing
    }

    val execId: String = startQueryExecution
    val lastQuery: LastQuery = pollingQueryExecution(execId)

    if (saveMode.equals(SaveMode.Overwrite)) {
      withS3(_.listObjectsV2(output.getBucket, output.getKey)).getObjectSummaries.asScala
        .filterNot(_.getKey.startsWith(lastQuery.outputCsvUri.getKey)) // filter output.csv and output.csv.metadata
        .foreach { summary: S3ObjectSummary =>
          logger.info(s"[$operatorName] Delete s3://${summary.getBucketName}/${summary.getKey}")
          withS3(_.deleteObject(summary.getBucketName, summary.getKey))
        }
    }
    if (!keepMetadata) {
      logger.info(s"[$operatorName] Delete ${lastQuery.outputCsvMetadataUri.toString}.")
      withS3(_.deleteObject(lastQuery.outputCsvMetadataUri.getBucket, lastQuery.outputCsvMetadataUri.getKey))
    }

    logger.info(s"[$operatorName] Created ${lastQuery.outputCsvUri} (scan: ${lastQuery.scanBytes.orNull} bytes, time: ${lastQuery.execMillis.orNull}ms)")
    val p: Config = buildLastQueryParam(lastQuery)

    val builder = TaskResult.defaultBuilder(request)
    builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
    builder.storeParams(p)
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
    lastQueryParam.set("output", lastQuery.outputCsvUri.toString)
    lastQueryParam.set("scan_bytes", lastQuery.scanBytes.getOrElse(Optional.absent()))
    lastQueryParam.set("exec_millis", lastQuery.execMillis.getOrElse(Optional.absent()))
    lastQueryParam.set("state", lastQuery.state)
    lastQueryParam.set("state_change_reason", lastQuery.stateChangeReason.getOrElse(Optional.absent()))
    lastQueryParam.set("submitted_at", lastQuery.submittedAt.getOrElse(Optional.absent()))
    lastQueryParam.set("completed_at", lastQuery.completedAt.getOrElse(Optional.absent()))

    ret
  }
}
