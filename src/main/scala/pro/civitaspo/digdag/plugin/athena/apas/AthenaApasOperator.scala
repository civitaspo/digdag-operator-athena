package pro.civitaspo.digdag.plugin.athena.apas


import com.amazonaws.services.glue.model.{SerDeInfo, Table}
import com.google.common.base.Optional
import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{ImmutableTaskResult, OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._
import scala.util.Random


class AthenaApasOperator(operatorName: String,
                         context: OperatorContext,
                         systemConfig: Config,
                         templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    sealed abstract class SaveMode

    object SaveMode
    {
        final case object Overwrite
            extends SaveMode
        final case object SkipIfExists
            extends SaveMode
        final case object ErrorIfExists
            extends SaveMode

        def apply(mode: String): SaveMode =
        {
            mode match {
                case "overwrite"       => Overwrite
                case "skip_if_exists"  => SkipIfExists
                case "error_if_exists" => ErrorIfExists
                case unknown           => throw new ConfigException(s"[$operatorName] save_mode '$unknown' is unsupported.")
            }
        }
    }


    protected val queryOrFile: String = params.get("_command", classOf[String])
    protected val database: String = params.get("database", classOf[String])
    protected val table: String = params.get("table", classOf[String])
    protected val workGroup: Optional[String] = params.getOptional("workgroup", classOf[String])
    protected val partitionKv: Map[String, String] = params.getMap("partition_kv", classOf[String], classOf[String]).asScala.toMap
    protected val _format: Option[String] = Option(params.getOptional("format", classOf[String]).orNull())
    protected val _compression: Option[String] = Option(params.getOptional("compression", classOf[String]).orNull())
    protected val _fieldDelimiter: Option[String] = Option(params.getOptional("field_delimiter", classOf[String]).orNull())
    protected val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "overwrite"))
    protected val bucketedBy: Seq[String] = params.getListOrEmpty("bucketed_by", classOf[String]).asScala.toSeq
    protected val bucketCount: Optional[Int] = params.getOptional("bucket_count", classOf[Int])
    protected val additionalProperties: Map[String, String] = params.getMapOrEmpty("additional_properties", classOf[String], classOf[String]).asScala.toMap
    protected val ignoreSchemaDiff: Boolean = params.get("ignore_schema_diff", classOf[Boolean], false)
    protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena-apas")
    protected val timeout: DurationParam = params.get("timeout", classOf[DurationParam], DurationParam.parse("10m"))
    protected val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    protected lazy val location: String = {
        val l = Option(params.getOptional("location", classOf[String]).orNull()).getOrElse {
            aws.glue.partition.generateLocation(catalogId, database, table, partitionKv)
        }
        if (l.endsWith("/")) l
        else l + "/"
    }

    protected def prepareCondition(): Boolean =
    {
        if (!aws.glue.database.exists(catalogId, database)) throw new ConfigException(s"database $database does not exist.")
        if (!aws.glue.table.exists(catalogId, database, table)) throw new ConfigException(s"table $database.$table does not exist.")
        if (!aws.glue.table.isPartitioned(catalogId, database, table)) throw new ConfigException(s"table $database.$table is not partitioned.")

        saveMode match {
            case SaveMode.Overwrite =>
                if (aws.glue.partition.exists(catalogId, database, table, partitionKv)) {
                    logger.info(s"Drop the partition{${partitionKv.mkString(",")}")
                    aws.glue.partition.delete(catalogId, database, table, partitionKv)
                }
                if (aws.s3.hasObjects(location)) {
                    logger.info(s"Delete objects in location $location.")
                    aws.s3.rm_r(location).foreach(uri => logger.info(s"Deleted: ${uri.toString}"))
                }

            case SaveMode.SkipIfExists =>
                if (aws.glue.partition.exists(catalogId, database, table, partitionKv)) {
                    logger.info(s"The partition {${partitionKv.mkString(",")} exists.")
                    return false
                }
                if (aws.s3.hasObjects(location)) {
                    logger.info(s"Objects exist in location $location.")
                    return false
                }

            case SaveMode.ErrorIfExists =>
                if (aws.glue.partition.exists(catalogId, database, table, partitionKv)) {
                    throw new ConfigException(s"The partition {${partitionKv.mkString(",")} already exists.")
                }
                if (aws.s3.hasObjects(location)) {
                    throw new ConfigException(s"Objects already exist in location $location.")
                }
        }
        true
    }

    override def runTask(): TaskResult =
    {
        if (!prepareCondition()) return TaskResult.empty(cf)

        val t: Table = aws.glue.table.describe(catalogId, database, table)
        val format: String = _format.getOrElse(detectFormat(t.getStorageDescriptor.getSerdeInfo.getSerializationLibrary))
        val compression: Option[String] = _compression.orElse(detectCompression(t.getParameters.asScala.toMap))
        val fieldDelimiter: Option[String] = _fieldDelimiter.orElse(detectFieldDelimiter(t.getStorageDescriptor.getSerdeInfo))

        val dummyTable: String = genDummyTableName()

        val subTask: Config = cf.create()
        subTask.set("+create-empty-dummy", buildCtasSubTaskConfig(tableName = dummyTable,
                                                                  format = format,
                                                                  compression = compression,
                                                                  fieldDelimiter = fieldDelimiter,
                                                                  tableMode = "empty"))
        subTask.set("+diff-schema", buildDiffSchemaInternalSubTaskConfig(comparisonTableName = dummyTable))
        subTask.set("+drop-dummy", buildDropTableSubTaskConfig(tableName = dummyTable))
        subTask.set("+store-data-by-ctas", buildCtasSubTaskConfig(tableName = dummyTable,
                                                                  outputLocation = Option(location),
                                                                  format = format,
                                                                  compression = compression,
                                                                  fieldDelimiter = fieldDelimiter,
                                                                  tableMode = "data_only"))
        subTask.set("+add-partition", buildAddPartitionSubTaskConfig())

        val builder: ImmutableTaskResult.Builder = TaskResult.defaultBuilder(cf)
        builder.subtaskConfig(subTask)
        builder.build()
    }

    protected def detectFormat(serDeLib: String): String =
    {
        serDeLib match {
            case "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe" => "parquet"
            case "org.apache.hadoop.hive.ql.io.orc.OrcSerde"                   => "orc"
            case "org.apache.hadoop.hive.serde2.avro.AvroSerDe"                => "avro"
            case "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"          => "textfile"
            case "org.apache.hadoop.hive.serde2.OpenCSVSerde"                  => "textfile"
            case "org.apache.hive.hcatalog.data.JsonSerDe"                     => "json"
            case "org.openx.data.jsonserde.JsonSerDe"                          => "json"
            case "com.amazon.emr.hive.serde.CloudTrailSerde"                   => "json"
            case "com.amazonaws.glue.serde.GrokSerDe"                          => "textfile"
            case "org.apache.hadoop.hive.serde2.RegexSerDe"                    => "textfile"
            case unknownSerDeLib                                               =>
                logger.warn(s"Unknown SerDe Lib: $unknownSerDeLib, use textfile")
                "textfile"
        }
    }

    protected def detectCompression(tableProperties: Map[String, String]): Option[String] =
    {
        if (tableProperties.contains("parquet.compression")) return Option(tableProperties("parquet.compression"))
        if (tableProperties.contains("orc.compression")) return Option(tableProperties("orc.compression"))
        None
    }

    protected def detectFieldDelimiter(serDeInfo: SerDeInfo): Option[String] =
    {
        Option(serDeInfo.getParameters.getOrDefault("field.delim", null))
    }

    protected def genDummyTableName(): String =
    {
        val normalizedSessionUuid: String = sessionUuid.replaceAll("-", "")
        val random: String = Random.alphanumeric.take(5).mkString
        s"digdag_athena_apas_${normalizedSessionUuid}_$random"
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

    protected def buildCtasSubTaskConfig(tableName: String,
                                         outputLocation: Option[String] = None,
                                         format: String,
                                         compression: Option[String] = None,
                                         fieldDelimiter: Option[String] = None,
                                         tableMode: String): Config =
    {
        val subTask: Config = cf.create()

        subTask.set("_type", "athena.ctas")
        subTask.set("_command", queryOrFile)
        subTask.set("database", database)
        subTask.set("table", tableName)
        if (workGroup.isPresent) subTask.set("workgroup", workGroup.get())
        outputLocation.foreach(ol => subTask.set("output", ol))
        subTask.set("format", format)
        compression.foreach(c => subTask.set("compression", c))
        fieldDelimiter.foreach(fd => subTask.set("field_delimiter", fd))
        if (bucketedBy.nonEmpty) subTask.set("bucketed_by", bucketedBy)
        if (bucketCount.isPresent) subTask.set("bucket_count", bucketCount.get())
        if (additionalProperties.nonEmpty) subTask.set("additional_properties", additionalProperties)
        subTask.set("table_mode", tableMode)
        subTask.set("save_mode", "overwrite")
        subTask.set("token_prefix", tokenPrefix)
        subTask.set("timeout", timeout.toString)

        putCommonSettingToSubTask(subTask)

        subTask
    }

    protected def buildDiffSchemaInternalSubTaskConfig(comparisonTableName: String): Config =
    {
        val subTask: Config = cf.create()

        subTask.set("_type", "athena.diff_schema_internal")
        subTask.set("database", database)
        subTask.set("original", table)
        subTask.set("comparison", comparisonTableName)
        catalogId.foreach(cid => subTask.set("catalog_id", cid))
        subTask.set("error_if_diff_found", !ignoreSchemaDiff)

        putCommonSettingToSubTask(subTask)

        subTask
    }

    protected def buildDropTableSubTaskConfig(tableName: String): Config =
    {
        val subTask: Config = cf.create()

        subTask.set("_type", "athena.drop_table")
        subTask.set("database", database)
        subTask.set("table", tableName)
        subTask.set("with_location", true)
        catalogId.foreach(cid => subTask.set("catalog_id", cid))

        putCommonSettingToSubTask(subTask)

        subTask
    }

    protected def buildAddPartitionSubTaskConfig(): Config =
    {
        val subTask: Config = cf.create()

        subTask.set("_type", "athena.add_partition")
        subTask.set("database", database)
        subTask.set("table", table)
        subTask.set("location", location)
        subTask.set("partition_kv", partitionKv.asJava)
        subTask.set("save_mode", "overwrite")
        subTask.set("follow_location", true)
        catalogId.foreach(cid => subTask.set("catalog_id", cid))

        putCommonSettingToSubTask(subTask)

        subTask
    }
}
