package pro.civitaspo.digdag.plugin.athena.operator

import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.base.Optional
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}

import scala.util.Try
import scala.util.hashing.MurmurHash3

class AthenaQueryOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
  extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  protected val queryOrFile: String = params.get("_command", classOf[String])
  protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena")
  protected val database: Optional[String] = params.getOptional("database", classOf[String])
  protected val output: String = params.get("output", classOf[String])

  protected lazy val query: String = {
    val t = Try {
      val f = workspace.getFile(queryOrFile)
      workspace.templateFile(templateEngine, f.getPath, UTF_8, params)
    }
    t.getOrElse(queryOrFile)
  }

  protected lazy val clientRequestToken: String = {
    val queryHash: Int = MurmurHash3.bytesHash(query.getBytes(UTF_8), 0)
    s"$tokenPrefix-$sessionUuid-$queryHash"
  }

  override def runTask(): TaskResult = null
}
