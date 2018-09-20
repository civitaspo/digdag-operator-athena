package pro.civitaspo.digdag.plugin.athena.operator
import com.amazonaws.services.s3.AmazonS3URI
import io.digdag.client.config.Config
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}

class AthenaRemoveMetadataOperator(operatorName: String, context: OperatorContext, systemConfig: Config, templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine) {

  object AmazonS3URI {
    def apply(path: String): AmazonS3URI = new AmazonS3URI(path, false)
  }

  protected val metadataUri: AmazonS3URI = AmazonS3URI(params.get("_command", classOf[String]))

  override def runTask(): TaskResult = {
    logger.info(s"[$operatorName] Delete ${metadataUri.toString}.")
    withS3(_.deleteObject(metadataUri.getBucket, metadataUri.getKey))
    TaskResult.empty(request)
  }
}
