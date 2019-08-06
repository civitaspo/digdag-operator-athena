package pro.civitaspo.digdag.plugin.athena.each_database


import com.amazonaws.services.glue.model.Database
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.jdk.CollectionConverters._
import scala.util.chaining._


class AthenaEachDatabaseOperator(operatorName: String,
                                 context: OperatorContext,
                                 systemConfig: Config,
                                 templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    protected val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())
    protected val parallelSlice: Int = params.get("parallel_slice", classOf[Int], 1)
    protected val doConfig: Config = params.getNested("_do")

    override def runTask(): TaskResult =
    {
        val doConfigs = aws.glue.database.list(catalogId).map { db =>
            cf.create().tap { newDoConfig =>
                newDoConfig.getNestedOrSetEmpty(s"+${db.getName}").tap { c =>
                    c.setAll(doConfig)
                    c.setNested("_export", convertDatabaseToExport(db))
                }
            }
        }

        val builder = TaskResult.defaultBuilder(cf)
        builder.subtaskConfig(generateParallelTasks(doConfigs = doConfigs))
        builder.build()
    }

    protected def generateParallelTasks(doConfigs: Seq[Config]): Config =
    {
        def gen(remainingDoConfigs: Seq[Config],
                result: Config = cf.create(),
                idx: Int = 0): Config =
        {
            result.tap { r =>
                val (left, right) = remainingDoConfigs.splitAt(parallelSlice)
                val subTaskConfig = cf.create().tap { c =>
                    val taskGroup: Config = c.getNestedOrSetEmpty(s"+p$idx")
                    taskGroup.set("_parallel", true)
                    left.foreach(taskGroup.setAll)
                }
                r.setAll(subTaskConfig)
                if (right.nonEmpty) gen(remainingDoConfigs = right, result = r, idx = idx + 1)
            }
        }

        gen(remainingDoConfigs = doConfigs)
    }

    protected def convertDatabaseToExport(database: Database): Config =
    {
        cf.create().tap { ret =>
            val export = ret
                .getNestedOrSetEmpty("athena")
                .getNestedOrSetEmpty("each_database")
                .getNestedOrSetEmpty("export")

            export.set("name", database.getName)
            Option(database.getCreateTime).foreach(ct => export.set("created_at", ct.toInstant.toEpochMilli))
            Option(database.getDescription).foreach(desc => export.set("description", desc))
            Option(database.getParameters).foreach { p =>
                p.asScala.foreach {
                    case (k: String, v: String) => export.getNestedOrSetEmpty("parameters").set(k, v)
                }
            }
        }
    }
}
