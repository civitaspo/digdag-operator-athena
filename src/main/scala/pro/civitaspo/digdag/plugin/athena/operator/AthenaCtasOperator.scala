package pro.civitaspo.digdag.plugin.athena.operator

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.amazonaws.services.athena.model.{QueryExecution, QueryExecutionState}
import com.amazonaws.services.s3.AmazonS3URI
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest
import com.google.common.base.Optional
import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam

import scala.collection.JavaConverters._
import scala.util.Try

class AthenaCtasOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  object AmazonS3URI {
    def apply(path: String): AmazonS3URI = new AmazonS3URI(path, false)
  }

  sealed abstract class SaveMode

  object SaveMode {
    final case object Default extends SaveMode
    final case object EmptyTable extends SaveMode
    final case object DataOnly extends SaveMode

    def apply(mode: String): SaveMode = {
      mode match {
        case "default" => Default
        case "empty_table" => EmptyTable
        case "data_only" => DataOnly
        case unknown => throw new ConfigException(s"[$operatorName] save_mode '$unknown' is unsupported.")
      }
    }
  }

  sealed abstract class QueryMode

  object QueryMode {
    final case object None extends QueryMode
    final case object ErrorIfExists extends QueryMode
    final case object Ignore extends QueryMode
    final case object Overwrite extends QueryMode

    def apply(mode: String): QueryMode = {
      mode match {
        case "none" => None
        case "error_if_exists" => ErrorIfExists
        case "ignore" => Ignore
        case "overwrite" => Overwrite
        case unknown => throw new ConfigException(s"[$operatorName] query_mode '$unknown' is unsupported.")
      }
    }
  }

  case class LastCtasQuery(
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

  object LastCtasQuery {

    def apply(qe: QueryExecution): LastCtasQuery = {
      new LastCtasQuery(
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

  protected val selectQueryOrFile: String = params.get("select_query", classOf[String])
  protected val database: Optional[String] = params.getOptional("database", classOf[String])
  protected val table: String = params.get("table", classOf[String], s"digdag-athena-ctas-$sessionUuid")
  protected val output: Optional[String] = params.getOptional("output", classOf[String]) // gen default later
  protected val format: String = params.get("format", classOf[String], "parquet")
  protected val compression: String = params.get("compression", classOf[String], "snappy")
  protected val fieldDelimiter: Optional[String] = params.getOptional("field_delimiter", classOf[String])
  protected val partitionedBy: Seq[String] = params.getListOrEmpty("partitioned_by", classOf[String]).asScala
  protected val bucketedBy: Seq[String] = params.getListOrEmpty("bucketed_by", classOf[String]).asScala
  protected val bucketCount: Optional[Int] = params.getOptional("bucket_count", classOf[Int])
  protected val additionalProperties: Map[String, String] = params.getMapOrEmpty("additional_properties", classOf[String], classOf[String]).asScala.toMap
  protected val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "default"))
  protected val queryMode: QueryMode = QueryMode(params.get("query_mode", classOf[String], "overwrite"))
  protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena-ctas")
  protected val timeout: DurationParam = params.get("timeout", classOf[DurationParam], DurationParam.parse("10m"))

  protected lazy val selectQuery: String = {
    val t: Try[String] = Try {
      val f: File = workspace.getFile(selectQueryOrFile)
      workspace.templateFile(templateEngine, f.getPath, UTF_8, params)
    }
    t.getOrElse(selectQueryOrFile)
  }

  override def runTask(): TaskResult = {
    null
  }
}
