package pro.civitaspo.digdag.plugin.athena.preview


import com.amazonaws.services.athena.model.ResultSet
import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._
import scala.util.Try

class AthenaPreviewOperator(operatorName: String,
                            context: OperatorContext,
                            systemConfig: Config,
                            templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{

    protected val executionId: String = params.get("_command", classOf[String])
    protected val maxRows: Int = params.get("max_rows", classOf[Int], 10)

    protected case class LastPreview(id: String,
                                     columns: Seq[LastPreviewColumnInfo],
                                     rows: Seq[Seq[String]] // TODO: Support types JSON can express
                                    )

    protected case class LastPreviewColumnInfo(caseSensitive: Option[Boolean],
                                               catalog: Option[String],
                                               label: Option[String],
                                               name: String,
                                               nullable: Option[String],
                                               precision: Option[Int],
                                               scale: Option[Int],
                                               database: Option[String],
                                               table: Option[String],
                                               `type`: String)

    protected object LastPreview
    {

        def apply(id: String,
                  rs: ResultSet): LastPreview =
        {
            new LastPreview(
                id = id,
                columns = rs.getResultSetMetadata.getColumnInfo.asScala.toSeq.map { ci =>
                    LastPreviewColumnInfo(
                        caseSensitive = Try(Option(Boolean.unbox(ci.getCaseSensitive))).getOrElse(None),
                        catalog = Try(Option(ci.getCatalogName)).getOrElse(None),
                        label = Try(Option(ci.getLabel)).getOrElse(None),
                        name = ci.getName,
                        nullable = Try(Option(ci.getNullable)).getOrElse(None),
                        precision = Try(Option(ci.getPrecision.toInt)).getOrElse(None),
                        scale = Try(Option(ci.getScale.toInt)).getOrElse(None),
                        database = Try(Option(ci.getSchemaName)).getOrElse(None),
                        table = Try(Option(ci.getTableName)).getOrElse(None),
                        `type` = ci.getType
                        )
                },
                rows = rs.getRows.asScala.toSeq.map(_.getData.asScala.toSeq.map(_.getVarCharValue)).tail // the first row is column names
                )
        }
    }

    override def runTask(): TaskResult =
    {
        val lastPreview: LastPreview = preview()

        val table = Tabulator.format(Seq(lastPreview.columns.map(_.name)) ++ lastPreview.rows)
        logger.info(s"[${operatorName}] Preview rows.\n$table")

        val p: Config = buildLastPreviewParam(lastPreview)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_preview")))
        builder.storeParams(p)
        builder.build()
    }

    protected def preview(): LastPreview =
    {
        val rs: ResultSet = aws.athena.preview(executionId, maxRows)
        LastPreview(executionId, rs)
    }

    protected def buildLastPreviewParam(lastPreview: LastPreview): Config =
    {
        val ret = cf.create()
        val lastPreviewParam = ret.getNestedOrSetEmpty("athena").getNestedOrSetEmpty("last_preview")

        lastPreviewParam.set("id", lastPreview.id)
        val columns = lastPreview.columns.map { ci =>
            val cp = cf.create()
            cp.set("case_sensitive", ci.caseSensitive.getOrElse(Optional.absent()))
            cp.set("catalog", ci.catalog.getOrElse(Optional.absent()))
            cp.set("label", ci.label.getOrElse(Optional.absent()))
            cp.set("name", ci.name)
            cp.set("nullable", ci.nullable.getOrElse(Optional.absent()))
            cp.set("precision", ci.precision.getOrElse(Optional.absent()))
            cp.set("scale", ci.scale.getOrElse(Optional.absent()))
            cp.set("database", ci.database.getOrElse(Optional.absent()))
            cp.set("table", ci.table.getOrElse(Optional.absent()))
            cp.set("type", ci.`type`)
        }
        lastPreviewParam.set("columns", columns.asJava)
        lastPreviewParam.set("rows", lastPreview.rows.map(_.asJava).asJava)

        ret
    }

    protected object Tabulator
    {

        def format(table: Seq[Seq[Any]]): String =
        {
            table match {
                case Seq() => ""
                case _     =>
                    val sizes = for (row <- table) yield for (cell <- row) yield if (cell == null) 0
                    else cell.toString.length
                    val colSizes = for (col <- sizes.transpose) yield col.max
                    val rows = for (row <- table) yield formatRow(row, colSizes)
                    formatRows(rowSeparator(colSizes), rows)
            }
        }

        def formatRows(rowSeparator: String,
                       rows: Seq[String]): String =
        {
            (rowSeparator ::
                rows.head ::
                rowSeparator ::
                rows.tail.toList :::
                rowSeparator ::
                List()).mkString("\n")
        }

        def formatRow(row: Seq[Any],
                      colSizes: Seq[Int]): String =
        {
            val cells = for ((item, size) <- row.zip(colSizes)) yield if (size == 0) ""
            else ("%" + size + "s").format(item)
            cells.mkString("|", "|", "|")
        }

        def rowSeparator(colSizes: Seq[Int]) =
        {
            colSizes map {
                "-" * _
            } mkString("+", "+", "+")
        }
    }
}
