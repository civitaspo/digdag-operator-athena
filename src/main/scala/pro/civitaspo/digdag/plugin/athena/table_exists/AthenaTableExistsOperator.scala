package pro.civitaspo.digdag.plugin.athena.table_exists


import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.util.{Failure, Success, Try}


class AthenaTableExistsOperator(operatorName: String,
                                context: OperatorContext,
                                systemConfig: Config,
                                templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{

    val database: String = params.get("database", classOf[String])
    val table: String = params.get("table", classOf[String])
    val withLocation: Boolean = params.get("with_location", classOf[Boolean], false)
    val errorIfNotExist: Boolean = params.get("error_if_not_exist", classOf[Boolean], false)
    val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    override def runTask(): TaskResult =
    {
        val storeParams: Config = cf.create()
        val lastTableExistsParams = storeParams.getNestedOrSetEmpty("athena").getNestedOrSetEmpty("last_table_exists")

        run(storeParams = lastTableExistsParams)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_table_exists")))
        builder.storeParams(storeParams)
        builder.build()
    }

    protected def run(storeParams: Config): Unit =
    {
        if (aws.glue.table.exists(catalogId, database, table)) {
            storeParams.set("table_exists", true)
        }
        else {
            if (errorIfNotExist) throw new IllegalStateException(s"The table '$database.$table' does not exist.")
            storeParams.set("table_exists", false)
        }

        if (withLocation) {
            Try(aws.glue.table.describe(catalogId, database, table)) match {
                case Success(t) =>
                    val location: String = {
                        val l = t.getStorageDescriptor.getLocation
                        if (l.endsWith("/")) l
                        else l + "/"
                    }
                    if (aws.s3.hasObjects(location)) {
                        storeParams.set("location_exists", true)
                    }
                    else {
                        if (errorIfNotExist) throw new IllegalStateException(s"The location '$location' does not exist, although the table '$database.$table' exists.")
                        storeParams.set("location_exists", false)
                    }

                case Failure(e) =>
                    logger.warn(s"The table '$database.$table' may not exist.", e)
                    storeParams.set("location_exists", false)
            }
        }
    }
}
