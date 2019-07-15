package pro.civitaspo.digdag.plugin.athena.drop_table


import com.amazonaws.services.glue.model.Table
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator


class AthenaDropTableOperator(operatorName: String,
                              context: OperatorContext,
                              systemConfig: Config,
                              templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    val database: String = params.get("database", classOf[String])
    val table: String = params.get("table", classOf[String])
    val withLocation: Boolean = params.get("with_location", classOf[Boolean], false)
    val ignoreIfNotExist: Boolean = params.get("ignore_if_not_exist", classOf[Boolean], true)
    val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    override def runTask(): TaskResult =
    {
        if (!aws.glue.table.exists(catalogId, database, table)) {
            if (!ignoreIfNotExist) throw new IllegalStateException(s"The table '$database.$table' does not exist.")
            return TaskResult.empty(cf)
        }

        if (withLocation) {
            val t: Table = aws.glue.table.describe(catalogId, database, table)
            val location: String = {
                val l = t.getStorageDescriptor.getLocation
                if (l.endsWith("/")) l
                else l + "/"
            }
            if (aws.s3.hasObjects(location)) {
                logger.info(s"Delete objects because the location $location has objects.")
                aws.s3.rm_r(location).foreach(uri => logger.info(s"Deleted: ${uri.toString}"))
            }
        }

        logger.info(s"Delete the table '$database.$table'")
        aws.glue.table.delete(catalogId, database, table)
        TaskResult.empty(cf)
    }
}
