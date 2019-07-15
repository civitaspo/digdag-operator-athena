package pro.civitaspo.digdag.plugin.athena.drop_partition


import com.amazonaws.services.glue.model.Partition
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._


class AthenaDropPartitionOperator(operatorName: String,
                                  context: OperatorContext,
                                  systemConfig: Config,
                                  templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    val database: String = params.get("database", classOf[String])
    val table: String = params.get("table", classOf[String])
    val partitionKv: Map[String, String] = params.getMap("partition_kv", classOf[String], classOf[String]).asScala.toMap
    val withLocation: Boolean = params.get("with_location", classOf[Boolean], false)
    val ignoreIfNotExist: Boolean = params.get("ignore_if_not_exist", classOf[Boolean], true)
    val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    override def runTask(): TaskResult =
    {
        if (!aws.glue.partition.exists(catalogId, database, table, partitionKv)) {
            if (!ignoreIfNotExist) throw new IllegalStateException(s"The partition{${partitionKv.mkString(",")}} does not exist.")
            return TaskResult.empty(cf)
        }

        if (withLocation) {
            val p: Partition = aws.glue.partition.describe(catalogId, database, table, partitionKv)
            val location: String = {
                val l = p.getStorageDescriptor.getLocation
                if (l.endsWith("/")) l
                else l + "/"
            }
            if (aws.s3.hasObjects(location)) {
                logger.info(s"Delete objects because the location $location has objects.")
                aws.s3.rm_r(location).foreach(uri => logger.info(s"Deleted: ${uri.toString}"))
            }
        }

        logger.info(s"Delete the partition{${partitionKv.mkString(",")}} in '$database.$table'")
        aws.glue.partition.delete(catalogId, database, table, partitionKv)
        TaskResult.empty(cf)
    }
}
