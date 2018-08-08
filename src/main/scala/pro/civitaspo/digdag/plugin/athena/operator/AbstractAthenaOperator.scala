package pro.civitaspo.digdag.plugin.athena.operator

import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, SecretProvider, TemplateEngine}
import io.digdag.util.BaseOperator
import org.slf4j.{Logger, LoggerFactory}

abstract class AbstractAthenaOperator (operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends BaseOperator(context) {

  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)
  protected val params: Config = {
    val elems: Seq[String] = operatorName.split("\\.")
    elems.indices.foldLeft(request.getConfig) { (p: Config, idx: Int) =>
      p.mergeDefault((0 to idx).foldLeft(request.getConfig) { (nestedParam: Config, keyIdx: Int) =>
        nestedParam.getNestedOrGetEmpty(elems(keyIdx))
      })
    }
  }
  protected val secrets: SecretProvider = context.getSecrets.getSecrets("athena")

}
