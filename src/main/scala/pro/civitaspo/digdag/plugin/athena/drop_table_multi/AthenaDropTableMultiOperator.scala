package pro.civitaspo.digdag.plugin.athena.drop_table_multi


import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator


class AthenaDropTableMultiOperator(operatorName: String,
                                   context: OperatorContext,
                                   systemConfig: Config,
                                   templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    val database: String = params.get("database", classOf[String])
    val regexp: String = params.getOptional("regexp", classOf[String]).orNull()
    val limit: Option[Int] = Option(params.getOptional("limit", classOf[Int]).orNull())
    val withLocation: Boolean = params.get("with_location", classOf[Boolean], false)
    val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    override def runTask(): TaskResult =
    {
        logger.info(s"Drop tables matched by the expression: /$regexp/ in $database")
        aws.glue.table.list(catalogId, database, Option(regexp), limit).foreach { t =>
            if (withLocation) {
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
            logger.info(s"Drop the table '$database.${t.getName}'")
            aws.glue.table.delete(catalogId, database, t.getName)
        }
        TaskResult.empty(cf)
    }

}
