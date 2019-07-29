package pro.civitaspo.digdag.plugin.athena.partition_exists


import com.amazonaws.services.glue.model.Partition
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}


class AthenaPartitionExistsOperator(operatorName: String,
                                    context: OperatorContext,
                                    systemConfig: Config,
                                    templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    protected val database: String = params.get("database", classOf[String])
    protected val table: String = params.get("table", classOf[String])
    protected val partitionKv: Map[String, String] = params.getMap("partition_kv", classOf[String], classOf[String]).asScala.toMap
    protected val withLocation: Boolean = params.get("with_location", classOf[Boolean], false)
    protected val errorIfNotExist: Boolean = params.get("error_if_not_exist", classOf[Boolean], false)
    protected val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())


    override def runTask(): TaskResult =
    {
        val storeParams: Config = cf.create()
        val lastPartitionExistsParams = storeParams.getNestedOrSetEmpty("athena").getNestedOrSetEmpty("last_partition_exists")

        run(storeParams = lastPartitionExistsParams)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_partition_exists")))
        builder.storeParams(storeParams)
        builder.build()
    }

    protected def run(storeParams: Config): Unit =
    {
        if (aws.glue.partition.exists(catalogId, database, table, partitionKv)) {
            storeParams.set("partition_exists", true)
        }
        else {
            if (errorIfNotExist) throw new IllegalStateException(s"The partition{${partitionKv.mkString(",")}} does not exist.")
            storeParams.set("partition_exists", false)
        }

        if (withLocation) {
            Try(aws.glue.partition.describe(catalogId, database, table, partitionKv)) match {
                case Success(p) =>
                    val location: String = {
                        val l = p.getStorageDescriptor.getLocation
                        if (l.endsWith("/")) l
                        else l + "/"
                    }
                    if (aws.s3.hasObjects(location)) {
                        storeParams.set("location_exists", true)
                    }
                    else {
                        if (errorIfNotExist) throw new IllegalStateException(s"The location '$location' does not exist, although the partition{${partitionKv.mkString(",")}} exists.")
                        storeParams.set("location_exists", false)
                    }

                case Failure(e) =>
                    logger.warn(s"The partition{${partitionKv.mkString(",")}} may not exist.", e)
                    storeParams.set("location_exists", false)
            }
        }
    }
}
