package pro.civitaspo.digdag.plugin.athena.operator
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}

class AthenaCtasOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  override def runTask(): TaskResult = {
    null
  }
}
