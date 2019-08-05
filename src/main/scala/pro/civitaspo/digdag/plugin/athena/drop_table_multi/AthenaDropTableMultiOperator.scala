package pro.civitaspo.digdag.plugin.athena.drop_table_multi


import java.util.Date

import com.amazonaws.services.glue.model.Table
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator


class AthenaDropTableMultiOperator(operatorName: String,
                                   context: OperatorContext,
                                   systemConfig: Config,
                                   templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    val database: String = params.get("database", classOf[String])
    val regexp: String = params.getOptional("regexp", classOf[String]).orNull()
    val protect: Option[Config] = Option(params.getOptionalNested("protect").orNull())
    val limit: Option[Int] = Option(params.getOptional("limit", classOf[Int]).orNull())
    val withLocation: Boolean = params.get("with_location", classOf[Boolean], false)
    val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())

    val now: Long = System.currentTimeMillis()

    override def runTask(): TaskResult =
    {
        logger.info(s"Drop tables matched by the expression: /$regexp/ in $database")
        aws.glue.table.list(catalogId, database, Option(regexp), limit).foreach { t =>
            if (!isProtected(t)) {
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
        }
        TaskResult.empty(cf)
    }

    protected def isProtected(t: Table): Boolean =
    {
        protect match {
            case None    => return false
            case Some(c) =>
                Option(c.getOptional("created_within", classOf[DurationParam]).orNull()) match {
                    case None    => // do nothing
                    case Some(d) =>
                        Option(t.getCreateTime).foreach { date =>
                            if (isDateWithin(date, d)) {
                                logger.info(s"Protect the table ${t.getDatabaseName}.${t.getName} because this is created" +
                                                s" within ${d.toString} (created at ${t.getCreateTime.toString}).")
                                return true
                            }
                        }
                }
                Option(c.getOptional("updated_within", classOf[DurationParam]).orNull()) match {
                    case None    => // do nothing
                    case Some(d) =>
                        Option(t.getUpdateTime).foreach { date =>
                            if (isDateWithin(date, d)) {
                                logger.info(s"Protect the table ${t.getDatabaseName}.${t.getName} because this is updated" +
                                                s" within ${d.toString} (updated at ${t.getUpdateTime.toString}).")
                                return true
                            }
                        }
                }
                Option(c.getOptional("accessed_within", classOf[DurationParam]).orNull()) match {
                    case None    => // do nothing
                    case Some(d) =>
                        Option(t.getLastAccessTime).foreach { date =>
                            if (isDateWithin(date, d)) {
                                logger.info(s"Protect the table ${t.getDatabaseName}.${t.getName} because this is accessed" +
                                                s" within ${d.toString} (last accessed at ${t.getLastAccessTime.toString}).")
                                return true
                            }
                        }
                }
        }
        false
    }

    protected def isDateWithin(target: Date,
                               durationWithin: DurationParam): Boolean =
    {
        val dateWithin: Date = new Date(now - durationWithin.getDuration.toMillis)
        target.after(dateWithin)
    }

}
