package pro.civitaspo.digdag.plugin.athena.operator

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.amazonaws.services.s3.AmazonS3URI
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

  sealed abstract class TableMode

  object TableMode {
    final case object Default extends TableMode
    final case object Empty extends TableMode
    final case object DataOnly extends TableMode

    def apply(mode: String): TableMode = {
      mode match {
        case "default" => Default
        case "empty" => Empty
        case "data_only" => DataOnly
        case unknown => throw new ConfigException(s"[$operatorName] table_mode '$unknown' is unsupported.")
      }
    }
  }

  sealed abstract class SaveMode

  object SaveMode {
    final case object None extends SaveMode
    final case object ErrorIfExists extends SaveMode
    final case object Ignore extends SaveMode
    final case object Overwrite extends SaveMode

    def apply(mode: String): SaveMode = {
      mode match {
        case "none" => None
        case "error_if_exists" => ErrorIfExists
        case "ignore" => Ignore
        case "overwrite" => Overwrite
        case unknown => throw new ConfigException(s"[$operatorName] save_mode '$unknown' is unsupported.")
      }
    }
  }

  protected val selectQueryOrFile: String = params.get("select_query", classOf[String])
  protected val database: Optional[String] = params.getOptional("database", classOf[String])
  protected val table: String = params.get("table", classOf[String], s"digdag-athena-ctas-$sessionUuid")
  protected val output: Optional[String] = params.getOptional("output", classOf[String])
  protected val format: String = params.get("format", classOf[String], "parquet")
  protected val compression: String = params.get("compression", classOf[String], "snappy")
  protected val fieldDelimiter: Optional[String] = params.getOptional("field_delimiter", classOf[String])
  protected val partitionedBy: Seq[String] = params.getListOrEmpty("partitioned_by", classOf[String]).asScala
  protected val bucketedBy: Seq[String] = params.getListOrEmpty("bucketed_by", classOf[String]).asScala
  protected val bucketCount: Optional[Int] = params.getOptional("bucket_count", classOf[Int])
  protected val additionalProperties: Map[String, String] = params.getMapOrEmpty("additional_properties", classOf[String], classOf[String]).asScala.toMap
  protected val tableMode: TableMode = TableMode(params.get("table_mode", classOf[String], "default"))
  protected val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "overwrite"))
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

  protected def generateCtasQuery(): String = {
    val propsBuilder = Map.newBuilder[String, String]
    if (output.isPresent) propsBuilder += ("external_location" -> s"'${output.get}'")
    propsBuilder += ("format" -> s"'$format'")
    format match {
      case "parquet" => propsBuilder += ("parquet_compression" -> s"'$compression'")
      case "orc" => propsBuilder += ("orc_compression" -> s"'$compression'")
      case _ => logger.info(s"compression is not supported for format: $format.")
    }
    if (fieldDelimiter.isPresent) propsBuilder += ("field_delimiter" -> s"'${fieldDelimiter.get}'")
    if (partitionedBy.nonEmpty) propsBuilder += ("partitioned_by" -> s"ARRAY[${partitionedBy.map(s => s"'$s'").mkString(",")}]")
    if (bucketedBy.nonEmpty) {
      propsBuilder += ("bucketed_by" -> s"ARRAY[${bucketedBy.map(s => s"'$s'").mkString(",")}]")
      if (!bucketCount.isPresent) throw new ConfigException(s"`bucket_count` must be set if `bucketed_by` is set.")
      propsBuilder += ("bucket_count" -> s"${bucketCount.get}")
    }
    if (additionalProperties.nonEmpty) propsBuilder ++= additionalProperties

    val propStr: String = propsBuilder.result().map { case (k, v) => s"$k = $v" }.mkString(",\n")
    val createTableClause: String = saveMode match {
      case SaveMode.Ignore => "CREATE TABLE IF NOT EXISTS"
      case _ => "CREATE TABLE"
    }
    val dataHint: String = tableMode match {
      case TableMode.Empty => "WITH NO DATA"
      case _ => "WITH DATA"
    }

    s""" -- GENERATED BY digdag athena.ctas> operator
       | $createTableClause "$table"
       | WITH (
       |   $propStr
       | )
       | AS
       | $selectQuery
       | $dataHint
     """.stripMargin
  }
}
