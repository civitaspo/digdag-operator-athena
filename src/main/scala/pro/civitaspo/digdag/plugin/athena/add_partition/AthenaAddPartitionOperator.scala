package pro.civitaspo.digdag.plugin.athena.add_partition


import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator


class AthenaAddPartitionOperator(operatorName: String,
                                 context: OperatorContext,
                                 systemConfig: Config,
                                 templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    override def runTask(): TaskResult =
    {
        null
    }
}
