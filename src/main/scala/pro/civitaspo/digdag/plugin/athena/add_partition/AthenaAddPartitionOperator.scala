package pro.civitaspo.digdag.plugin.athena.add_partition


import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._


class AthenaAddPartitionOperator(operatorName: String,
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

    val database: String = params.get("database", classOf[String])
    val table: String = params.get("table", classOf[String])
    val partitionKv: Map[String, String] = params.getMap("partition_kv", classOf[String], classOf[String]).asScala.toMap
    val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "overwrite"))
    val followLocation: Boolean = params.get("follow_location", classOf[Boolean], true)
    val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    lazy val location: String = {
        val l = Option(params.getOptional("location", classOf[String]).orNull()).getOrElse {
            aws.glue.partition.generateLocation(catalogId, database, table, partitionKv)
        }
        if (l.endsWith("/")) l
        else l + "/"
    }

    def validateOptions(): Unit =
    {
        if (!aws.glue.database.exists(catalogId, database)) throw new ConfigException(s"database $database does not exist.")
        if (!aws.glue.table.exists(catalogId, database, table)) throw new ConfigException(s"table $database.$table does not exist.")
        if (!aws.glue.table.isPartitioned(catalogId, database, table)) throw new ConfigException(s"table $database.$table is not partitioned.")
    }

    override def runTask(): TaskResult =
    {
        validateOptions()
        if (!aws.s3.hasObjects(location)) {
            if (followLocation) {
                doFollowLocation()
                return TaskResult.empty(cf)
            }
            logger.warn(s"Create a new partition or update the existing partition, but location $location does not exist.")
        }

        if (!aws.glue.partition.exists(catalogId, database, table, partitionKv)) {
            createPartition()
            return TaskResult.empty(cf)
        }

        updatePartition()
        TaskResult.empty(cf)
    }

    protected def doFollowLocation(): Unit =
    {
        if (!aws.glue.partition.exists(catalogId, database, table, partitionKv)) {
            logger.info(s"location: $location does not exist, so skip to add a partition.")
        }
        else {
            saveMode match {
                case SaveMode.Overwrite =>
                    logger.info(s"Delete the partition${partitionKv.toString()} because the location $location does not exist.")
                    aws.glue.partition.delete(catalogId, database, table, partitionKv)

                case SaveMode.SkipIfExists =>
                    logger.warn(s"The partition${partitionKv.toString()} should be deleted because the location $location does not exist.")

                case SaveMode.ErrorIfExists =>
                    throw new IllegalStateException(s"The partition${partitionKv.toString()} exists.")
            }
        }
    }

    protected def createPartition(): Unit =
    {
        logger.info(s"Add a new partition${partitionKv.toString()} (location: $location) for '$database.$table'.")
        aws.glue.partition.add(catalogId, database, table, partitionKv, Option(location))
    }

    protected def updatePartition(): Unit =
    {
        saveMode match {
            case SaveMode.Overwrite =>
                logger.info(s"Add the partition${partitionKv.toString()} (location: $location) for '$database.$table'.")
                aws.glue.partition.update(catalogId, database, table, partitionKv, Option(location))

            case SaveMode.SkipIfExists =>
                logger.info(s"Skip to update the partition because the partition${partitionKv.toString()} already exists.")

            case SaveMode.ErrorIfExists =>
                throw new IllegalStateException(s"The partition${partitionKv.toString()} already exists.")
        }
    }
}
