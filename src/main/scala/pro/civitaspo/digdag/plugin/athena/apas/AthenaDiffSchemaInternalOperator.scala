package pro.civitaspo.digdag.plugin.athena.apas


import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._


class AthenaDiffSchemaInternalOperator(operatorName: String,
                                       context: OperatorContext,
                                       systemConfig: Config,
                                       templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    protected val database: String = params.get("database", classOf[String])
    protected val original: String = params.get("original", classOf[String])
    protected val comparison: String = params.get("comparison", classOf[String])
    protected val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())
    protected val errorIfDiffFound: Boolean = params.get("error_if_diff_found", classOf[Boolean], true)

    override def runTask(): TaskResult =
    {
        val o = aws.glue.table.describe(catalogId, database, original)
        val c = aws.glue.table.describe(catalogId, database, comparison)

        val originalColumns = o.getStorageDescriptor.getColumns.asScala.toSeq.diff(o.getPartitionKeys.asScala.toSeq).tapEach(_.setComment(null))
        val comparisonColumns = c.getStorageDescriptor.getColumns.asScala.toSeq.tapEach(_.setComment(null))

        val commonColumns = originalColumns.intersect(comparisonColumns)
        logger.info(s"Columns[${commonColumns.map(_.toString).mkString(",")}] are commonly in the tables $database.$comparison and $database.$original")

        val diffColumnsOandC = originalColumns.diff(comparisonColumns).tapEach(c => logger.warn(s"A column: ${c.toString} is not found in the table $database.$comparison but $database.$original has."))
        val diffColumnsCandO = comparisonColumns.diff(originalColumns).tapEach(c => logger.warn(s"A column: ${c.toString} is not found in the table $database.$original but $database.$comparison has."))

        if (diffColumnsOandC.nonEmpty) {
            val msg = s"Columns[${diffColumnsOandC.map(_.toString).mkString(",")}] are found in $database.$original" +
                s" but not found in $database.$comparison."
            if (errorIfDiffFound) throw new IllegalStateException(msg)
            else logger.warn(msg)
        }
        if (diffColumnsCandO.nonEmpty) {
            val msg = s"Columns[${diffColumnsCandO.map(_.toString).mkString(",")}] are found in $database.$comparison" +
                s" but not found in $database.$original."
            if (errorIfDiffFound) throw new IllegalStateException(msg)
            else logger.warn(msg)
        }

        TaskResult.empty(cf)
    }
}
