package pro.civitaspo.digdag.plugin.athena


import java.lang.reflect.Constructor
import java.util.{Arrays => JArrays, List => JList}

import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{Operator, OperatorContext, OperatorFactory, OperatorProvider, Plugin, TemplateEngine}
import javax.inject.Inject
import pro.civitaspo.digdag.plugin.athena.add_partition.AthenaAddPartitionOperator
import pro.civitaspo.digdag.plugin.athena.apas.{AthenaApasOperator, AthenaDiffSchemaInternalOperator}
import pro.civitaspo.digdag.plugin.athena.ctas.AthenaCtasOperator
import pro.civitaspo.digdag.plugin.athena.drop_partition.AthenaDropPartitionOperator
import pro.civitaspo.digdag.plugin.athena.drop_table.AthenaDropTableOperator
import pro.civitaspo.digdag.plugin.athena.drop_table_multi.AthenaDropTableMultiOperator
import pro.civitaspo.digdag.plugin.athena.each_database.AthenaEachDatabaseOperator
import pro.civitaspo.digdag.plugin.athena.partition_exists.AthenaPartitionExistsOperator
import pro.civitaspo.digdag.plugin.athena.preview.AthenaPreviewOperator
import pro.civitaspo.digdag.plugin.athena.query.AthenaQueryOperator
import pro.civitaspo.digdag.plugin.athena.table_exists.AthenaTableExistsOperator


object AthenaPlugin
{

    class AthenaOperatorProvider
        extends OperatorProvider
    {

        @Inject protected var systemConfig: Config = null
        @Inject protected var templateEngine: TemplateEngine = null

        override def get(): JList[OperatorFactory] =
        {
            JArrays.asList(
                operatorFactory("athena.add_partition", classOf[AthenaAddPartitionOperator]),
                operatorFactory("athena.drop_partition", classOf[AthenaDropPartitionOperator]),
                operatorFactory("athena.diff_schema_internal", classOf[AthenaDiffSchemaInternalOperator]),
                operatorFactory("athena.apas", classOf[AthenaApasOperator]),
                operatorFactory("athena.ctas", classOf[AthenaCtasOperator]),
                operatorFactory("athena.query", classOf[AthenaQueryOperator]),
                operatorFactory("athena.preview", classOf[AthenaPreviewOperator]),
                operatorFactory("athena.drop_table", classOf[AthenaDropTableOperator]),
                operatorFactory("athena.drop_table_multi", classOf[AthenaDropTableMultiOperator]),
                operatorFactory("athena.partition_exists?", classOf[AthenaPartitionExistsOperator]),
                operatorFactory("athena.table_exists?", classOf[AthenaTableExistsOperator]),
                operatorFactory("athena.each_database", classOf[AthenaEachDatabaseOperator])
                )
        }

        private def operatorFactory[T <: AbstractAthenaOperator](operatorName: String,
                                                                 klass: Class[T]): OperatorFactory =
        {
            new OperatorFactory
            {
                override def getType: String =
                {
                    operatorName
                }

                override def newOperator(context: OperatorContext): Operator =
                {
                    val constructor: Constructor[T] = klass.getConstructor(classOf[String],
                                                                           classOf[OperatorContext],
                                                                           classOf[Config],
                                                                           classOf[TemplateEngine])
                    try {
                        constructor.newInstance(operatorName, context, systemConfig, templateEngine)
                    }
                    catch {
                        case e: Throwable => throw new ConfigException(e)
                    }
                }
            }
        }
    }
}

class AthenaPlugin
    extends Plugin
{

    override def getServiceProvider[T](`type`: Class[T]): Class[_ <: T] =
    {
        if (`type` ne classOf[OperatorProvider]) return null
        classOf[AthenaPlugin.AthenaOperatorProvider].asSubclass(`type`)
    }
}
