package pro.civitaspo.digdag.plugin.athena.ctas


import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.base.Optional
import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{ImmutableTaskResult, OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.collection.JavaConverters._
import scala.util.{Failure, Random, Success, Try}

class AthenaCtasOperator(operatorName: String,
                         context: OperatorContext,
                         systemConfig: Config,
                         templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    sealed abstract class TableMode

    object TableMode
    {
        final case object Default
            extends TableMode
        final case object Empty
            extends TableMode
        final case object DataOnly
            extends TableMode

        def apply(mode: String): TableMode =
        {
            mode match {
                case "default"   => Default
                case "empty"     => Empty
                case "data_only" => DataOnly
                case unknown     => throw new ConfigException(s"[$operatorName] table_mode '$unknown' is unsupported.")
            }
        }
    }

    sealed abstract class SaveMode

    object SaveMode
    {
        final case object None
            extends SaveMode
        final case object ErrorIfExists
            extends SaveMode
        final case object Ignore
            extends SaveMode
        final case object Overwrite
            extends SaveMode

        def apply(mode: String): SaveMode =
        {
            mode match {
                case "none"            => None
                case "error_if_exists" => ErrorIfExists
                case "ignore"          => Ignore
                case "overwrite"       => Overwrite
                case unknown           => throw new ConfigException(s"[$operatorName] save_mode '$unknown' is unsupported.")
            }
        }
    }

    protected lazy val defaultTableName: String = {
        val normalizedSessionUuid: String = sessionUuid.replaceAll("-", "")
        val random: String = Random.alphanumeric.take(5).mkString
        s"digdag_athena_ctas_${normalizedSessionUuid}_$random"
    }

    @deprecated(message = "Use athena.ctas> instead.", since = "0.2.0")
    protected val _selectQueryOrFile: Optional[String] = params.getOptional("select_query", classOf[String])
    protected val selectQueryOrFile: String = {
        val _command = params.getOptional("_command", classOf[String])
        if (_selectQueryOrFile.isPresent && _command.isPresent) throw new ConfigException("Use athena.ctas> instead of 'select_query'")
        else if (_command.isPresent) _command.get()
        else {
            logger.warn("'select_query' is deprecated. Use athena.ctas> instead.")
            _selectQueryOrFile.get()
        }
    }
    protected val database: Optional[String] = params.getOptional("database", classOf[String])
    protected val table: String = params.get("table", classOf[String], defaultTableName)
    protected val workGroup: Optional[String] = params.getOptional("workgroup", classOf[String])
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
        val t: Try[String] =
            if (selectQueryOrFile.startsWith("s3://")) loadQueryOnS3(selectQueryOrFile)
            else loadQueryOnLocalFileSystem(selectQueryOrFile)

        t.getOrElse(selectQueryOrFile).replaceAll(";\\s+?$", "")
    }

    protected def loadQueryOnS3(uriString: String): Try[String] =
    {
        val t = Try {
            val content = aws.s3.readObject(uriString)
            templateEngine.template(content, params)
        }
        t match {
            case Success(_) => logger.info("Succeeded to load the query on S3.")
            case Failure(e) => logger.warn(s"Failed to load the query on S3.: ${e.getMessage}")
        }
        t
    }

    protected def loadQueryOnLocalFileSystem(path: String): Try[String] =
    {
        val t = Try {
            val f = workspace.getFile(path)
            workspace.templateFile(templateEngine, f.getPath, UTF_8, params)
        }
        t match {
            case Success(_) => logger.info("Succeeded to load the query on LocalFileSystem.")
            case Failure(e) => logger.warn(s"Failed to load the query on LocalFileSystem.: ${e.getMessage}")
        }
        t
    }

    override def runTask(): TaskResult =
    {
        saveMode match {
            case SaveMode.ErrorIfExists if output.isPresent && hasObjects(output.get) =>
                throw new IllegalStateException(s"${output.get} already exists")
            case SaveMode.Ignore if output.isPresent && hasObjects(output.get)        =>
                logger.info(s"${output.get} already exists, so ignore this session.")
                return TaskResult.empty(request)
            case SaveMode.Overwrite if output.isPresent                               =>
                logger.info(s"Overwrite ${output.get}")
                rmObjects(output.get)
            case _                                                                    => // do nothing
        }

        val subTask: Config = cf.create()
        if (saveMode.equals(SaveMode.Overwrite)) subTask.setNested("+drop-before-ctas", buildQuerySubTaskConfig(generateDropTableQuery()))
        subTask.setNested("+ctas", buildQuerySubTaskConfig(generateCtasQuery()))
        if (tableMode.equals(TableMode.DataOnly)) subTask.setNested("+drop-after-ctas", buildQuerySubTaskConfig(generateDropTableQuery()))

        val builder: ImmutableTaskResult.Builder = TaskResult.defaultBuilder(cf)
        builder.subtaskConfig(subTask)
        builder.build()
    }

    protected def hasObjects(location: String): Boolean =
    {
        aws.s3.ls(location).nonEmpty
    }

    protected def rmObjects(location: String): Unit =
    {
        aws.s3.rm_r(location).foreach(uri => logger.info(s"Deleted: ${uri.toString}"))
    }

    protected def generateCtasQuery(): String =
    {
        val propsBuilder = Map.newBuilder[String, String]
        if (output.isPresent) propsBuilder += ("external_location" -> s"'${output.get}'")
        propsBuilder += ("format" -> s"'$format'")
        format match {
            case "parquet" => propsBuilder += ("parquet_compression" -> s"'$compression'")
            case "orc"     => propsBuilder += ("orc_compression" -> s"'$compression'")
            case _         => logger.info(s"compression is not supported for format: $format.")
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
            case _               => "CREATE TABLE"
        }
        val dataHint: String = tableMode match {
            case TableMode.Empty => "WITH NO DATA"
            case _               => "WITH DATA"
        }

        s""" -- GENERATED BY digdag athena.ctas> operator
           | $createTableClause "$table"
           | WITH (
           |   $propStr
           | )
           | AS
           | $selectQuery
           | $dataHint
           | """.stripMargin
    }

    protected def generateDropTableQuery(): String =
    {
        s""" -- GENERATED BY digdag athena.ctas> operator
           | DROP TABLE IF EXISTS $table
           | """.stripMargin
    }

    protected def buildQuerySubTaskConfig(query: String): Config =
    {
        logger.info(s"Will execute query in athena.query>: $query")

        val subTask: Config = cf.create()

        subTask.set("_type", "athena.query")
        subTask.set("_command", query)
        subTask.set("token_prefix", tokenPrefix)
        if (database.isPresent) subTask.set("database", database)
        if (workGroup.isPresent) subTask.set("workgroup", workGroup)
        subTask.set("timeout", timeout.toString)
        subTask.set("preview", false)

        subTask.set("auth_method", aws.conf.authMethod)
        subTask.set("profile_name", aws.conf.profileName)
        if (aws.conf.profileFile.isPresent) subTask.set("profile_file", aws.conf.profileFile.get())
        subTask.set("use_http_proxy", aws.conf.useHttpProxy)
        subTask.set("region", aws.region)
        if (aws.conf.endpoint.isPresent) subTask.set("endpoint", aws.conf.endpoint.get())

        subTask
    }
}
