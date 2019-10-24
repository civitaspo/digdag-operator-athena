package pro.civitaspo.digdag.plugin.athena.copy_catalog


import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{OperatorContext, SecretProvider, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator
import pro.civitaspo.digdag.plugin.athena.aws.Aws
import pro.civitaspo.digdag.plugin.athena.util.SecretProviderChain

import scala.jdk.CollectionConverters._

class AthenaCopyCatalogOperator(operatorName: String,
                                context: OperatorContext,
                                systemConfig: Config,
                                templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    sealed abstract class SaveMode

    protected object SaveMode
    {
        final case object Overwrite
            extends SaveMode
        final case object SkipIfExists
            extends SaveMode
        final case object ErrorIfExists
            extends SaveMode

        def apply(mode: String): SaveMode =
        {
            mode match {
                case "overwrite"       => Overwrite
                case "skip_if_exists"  => SkipIfExists
                case "error_if_exists" => ErrorIfExists
                case unknown           => throw new ConfigException(s"[$operatorName] save_mode '$unknown' is unsupported.")
            }
        }
    }

    protected case class TargetInfo(database: String,
                                    table: String,
                                    partitionFilter: Option[String])

    protected val targets: Seq[TargetInfo] = params.getList("_command", classOf[Config]).asScala.toSeq.map { c =>
        TargetInfo(
            database = c.get("database", classOf[String]),
            table = c.get("table", classOf[String]),
            partitionFilter = Option(c.getOptional("partition_filter", classOf[String]).orNull())
            )
    }
    protected val catalog_id: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())
    protected val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "overwrite"))

    protected val xParams: Config = params.get("to", classOf[Config])
    protected val xSecrets: SecretProvider = secrets.getSecrets("x")
    protected val xCatalogId: Option[String] = Option(xParams.getOptional("catalog_id", classOf[String]).orNull())

    val xAws: Aws = Aws(
        aws.conf.copy(
            accessKeyId = xSecrets.getSecretOptional("access_key_id").or(aws.conf.accessKeyId),
            secretAccessKey = xSecrets.getSecretOptional("secret_access_key").or(aws.conf.secretAccessKey),
            sessionToken = xSecrets.getSecretOptional("session_token").or(aws.conf.sessionToken),
            roleArn = xSecrets.getSecretOptional("role_arn"),
            roleSessionName = xSecrets.getSecretOptional("role_session_name").or(s"digdag-athena-x-$sessionUuid"),
            webIdentityTokenFile = xParams.getOptional("web_identity_token_file", classOf[String]).or(aws.conf.webIdentityTokenFile),
            webIdentityRoleArn = xParams.getOptional("web_identity_role_arn", classOf[String]).or(aws.conf.webIdentityRoleArn),
            httpProxy = SecretProviderChain(xSecrets.getSecrets("http_proxy"), aws.conf.httpProxy),
            authMethod = xParams.get("auth_method", classOf[String], aws.conf.authMethod),
            profileName = xParams.get("profile_name", classOf[String], aws.conf.profileName),
            profileFile = xParams.getOptional("profile_file", classOf[String]).or(aws.conf.profileFile),
            useHttpProxy = xParams.get("use_http_proxy", classOf[Boolean], aws.conf.useHttpProxy),
            region = xParams.getOptional("region", classOf[String]).or(aws.conf.region),
            endpoint = xParams.getOptional("endpoint", classOf[String]).or(aws.conf.endpoint)
            )
        )

    override def runTask(): TaskResult =
    {
        null
    }
}
