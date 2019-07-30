package pro.civitaspo.digdag.plugin.athena.ctas


import java.nio.charset.StandardCharsets.UTF_8

import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{ImmutableTaskResult, OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._
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

    protected val DEFAULT_DATABASE_NAME = "default"

    protected lazy val defaultTableName: String = {
        val normalizedSessionUuid: String = sessionUuid.replaceAll("-", "")
        val random: String = Random.alphanumeric.take(5).mkString
        s"digdag_athena_ctas_${normalizedSessionUuid}_$random"
    }

    protected val selectQueryOrFile: String = params.get("_command", classOf[String])
    protected val database: Option[String] = Option(params.getOptional("database", classOf[String]).orNull())
    protected val table: String = params.get("table", classOf[String], defaultTableName)
    protected val workGroup: Option[String] = Option(params.getOptional("workgroup", classOf[String]).orNull())
    protected val location: Option[String] = {
        Option(params.getOptional("location", classOf[String]).orNull()) match {
            case Some(l) if !l.endsWith("/") => Option(s"$l/")
            case option: Option[String]      => option
        }
    }
    protected val format: String = params.get("format", classOf[String], "parquet")
    protected val compression: String = params.get("compression", classOf[String], "snappy")
    protected val fieldDelimiter: Option[String] = Option(params.getOptional("field_delimiter", classOf[String]).orNull())
    protected val partitionedBy: Seq[String] = params.getListOrEmpty("partitioned_by", classOf[String]).asScala.toSeq
    protected val bucketedBy: Seq[String] = params.getListOrEmpty("bucketed_by", classOf[String]).asScala.toSeq
    protected val bucketCount: Option[Int] = Option(params.getOptional("bucket_count", classOf[Int]).orNull())
    protected val additionalProperties: Map[String, String] = params.getMapOrEmpty("additional_properties", classOf[String], classOf[String]).asScala.toMap
    protected val tableMode: TableMode = TableMode(params.get("table_mode", classOf[String], "default"))
    protected val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "overwrite"))
    protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena-ctas")
    protected val timeout: DurationParam = params.get("timeout", classOf[DurationParam], DurationParam.parse("10m"))
    protected val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

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
            case SaveMode.ErrorIfExists if location.exists(aws.s3.hasObjects) =>
                throw new IllegalStateException(s"${location.get} already exists")

            case SaveMode.Ignore if location.exists(aws.s3.hasObjects) =>
                logger.info(s"${location.get} already exists, so ignore this session.")
                return TaskResult.empty(request)

            case SaveMode.Overwrite =>
                location.foreach { l =>
                    logger.info(s"Overwrite $l")
                    aws.s3.rm_r(l).foreach(uri => logger.info(s"Deleted: ${uri.toString}"))
                }

            case _ => // do nothing
        }

        val subTask: Config = cf.create()
        if (saveMode.equals(SaveMode.Overwrite)) subTask.setNested("+drop-before-ctas", buildDropTableSubTaskConfig())
        subTask.setNested("+ctas", buildCtasQuerySubTaskConfig())
        if (tableMode.equals(TableMode.DataOnly)) subTask.setNested("+drop-after-ctas", buildDropTableSubTaskConfig())

        val builder: ImmutableTaskResult.Builder = TaskResult.defaultBuilder(cf)
        builder.subtaskConfig(subTask)
        builder.build()
    }

    protected def generateCtasQuery(): String =
    {
        val propsBuilder = Map.newBuilder[String, String]
        location.foreach(l => propsBuilder += ("external_location" -> s"'$l'"))
        propsBuilder += ("format" -> s"'$format'")
        format match {
            case "parquet" => propsBuilder += ("parquet_compression" -> s"'$compression'")
            case "orc"     => propsBuilder += ("orc_compression" -> s"'$compression'")
            case _         => logger.info(s"compression is not supported for format: $format.")
        }
        fieldDelimiter.foreach(fd => propsBuilder += ("field_delimiter" -> s"'$fd'"))
        if (partitionedBy.nonEmpty) propsBuilder += ("partitioned_by" -> s"ARRAY[${partitionedBy.map(s => s"'$s'").mkString(",")}]")
        if (bucketedBy.nonEmpty) {
            propsBuilder += ("bucketed_by" -> s"ARRAY[${bucketedBy.map(s => s"'$s'").mkString(",")}]")
            if (bucketCount.isEmpty) throw new ConfigException(s"`bucket_count` must be set if `bucketed_by` is set.")
            bucketCount.foreach(bc => propsBuilder += ("bucket_count" -> s"$bc"))
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

    protected def putCommonSettingToSubTask(subTask: Config): Unit =
    {
        subTask.set("auth_method", aws.conf.authMethod)
        subTask.set("profile_name", aws.conf.profileName)
        if (aws.conf.profileFile.isPresent) subTask.set("profile_file", aws.conf.profileFile.get())
        subTask.set("use_http_proxy", aws.conf.useHttpProxy)
        subTask.set("region", aws.region)
        if (aws.conf.endpoint.isPresent) subTask.set("endpoint", aws.conf.endpoint.get())
    }

    protected def buildCtasQuerySubTaskConfig(): Config =
    {
        val subTask: Config = cf.create()

        subTask.set("_type", "athena.query")
        subTask.set("_command", generateCtasQuery())
        subTask.set("token_prefix", tokenPrefix)
        database.foreach(db => subTask.set("database", db))
        workGroup.foreach(wg => subTask.set("workgroup", wg))
        subTask.set("timeout", timeout.toString)
        subTask.set("preview", false)

        putCommonSettingToSubTask(subTask)

        subTask
    }

    protected def buildDropTableSubTaskConfig(): Config =
    {
        val subTask: Config = cf.create()

        subTask.set("_type", "athena.drop_table")
        subTask.set("database", database.getOrElse(DEFAULT_DATABASE_NAME))
        subTask.set("table", table)
        subTask.set("with_location", false)
        catalogId.foreach(cid => subTask.set("catalog_id", cid))

        putCommonSettingToSubTask(subTask)

        subTask
    }
}
