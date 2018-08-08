package pro.civitaspo.digdag.plugin.athena.operator

import com.google.common.base.Optional
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}

class AthenaQueryOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  val queryOrFile: String = params.get("_command", classOf[String])
  val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena")
  val database: Optional[String] = params.getOptional("database", classOf[String])
  val output: String = params.get("output", classOf[String])

  override def runTask(): TaskResult = null
}
