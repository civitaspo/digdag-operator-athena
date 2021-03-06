package pro.civitaspo.digdag.plugin.athena


import com.typesafe.scalalogging.LazyLogging
import io.digdag.client.config.{Config, ConfigFactory}
import io.digdag.spi.{OperatorContext, SecretProvider, TemplateEngine}
import io.digdag.util.{BaseOperator, DurationParam}
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsConf}


abstract class AbstractAthenaOperator(operatorName: String,
                                      context: OperatorContext,
                                      systemConfig: Config,
                                      templateEngine: TemplateEngine)
    extends BaseOperator(context)
        with LazyLogging
{
    if (!logger.underlying.isDebugEnabled) {
        // NOTE: suppress aws-java-sdk logs because of a bit noisy logging.
        System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.NoOpLog")
    }
    protected val cf: ConfigFactory = request.getConfig.getFactory
    protected val params: Config = {
        val elems: Seq[String] = operatorName.split("\\.").toSeq
        elems.indices.foldLeft(request.getConfig) { (p: Config,
                                                     idx: Int) =>
            p.mergeDefault((0 to idx).foldLeft(request.getConfig) { (nestedParam: Config,
                                                                     keyIdx: Int) =>
                nestedParam.getNestedOrGetEmpty(elems(keyIdx))
            })
        }
    }
    protected val secrets: SecretProvider = context.getSecrets.getSecrets("athena")
    protected val sessionUuid: String = params.get("session_uuid", classOf[String])

    protected val aws: Aws = Aws(
        AwsConf(
            isAllowedAuthMethodEnv = systemConfig.get("athena.allow_auth_method_env", classOf[Boolean], false),
            isAllowedAuthMethodInstance = systemConfig.get("athena.allow_auth_method_instance", classOf[Boolean], false),
            isAllowedAuthMethodProfile = systemConfig.get("athena.allow_auth_method_profile", classOf[Boolean], false),
            isAllowedAuthMethodProperties = systemConfig.get("athena.allow_auth_method_properties", classOf[Boolean], false),
            isAllowedAuthMethodWebIdentityToken = systemConfig.get("athena.allow_auth_method_web_identity_token", classOf[Boolean], false),
            assumeRoleTimeoutDuration = systemConfig.get("athena.assume_role_timeout_duration", classOf[DurationParam], DurationParam.parse("1h")),
            accessKeyId = secrets.getSecretOptional("access_key_id"),
            secretAccessKey = secrets.getSecretOptional("secret_access_key"),
            sessionToken = secrets.getSecretOptional("session_token"),
            roleArn = secrets.getSecretOptional("role_arn"),
            roleSessionName = secrets.getSecretOptional("role_session_name").or(s"digdag-athena-$sessionUuid"),
            defaultWebIdentityTokenFile = systemConfig.getOptional("athena.default_web_identity_token_file", classOf[String]),
            webIdentityTokenFile = params.getOptional("web_identity_token_file", classOf[String]),
            defaultWebIdentityRoleArn = systemConfig.getOptional("athena.default_web_identity_role_arn", classOf[String]),
            webIdentityRoleArn = params.getOptional("web_identity_role_arn", classOf[String]),
            httpProxy = secrets.getSecrets("http_proxy"),
            authMethod = params.get("auth_method", classOf[String], "basic"),
            profileName = params.get("profile_name", classOf[String], "default"),
            profileFile = params.getOptional("profile_file", classOf[String]),
            useHttpProxy = params.get("use_http_proxy", classOf[Boolean], false),
            region = params.getOptional("region", classOf[String]),
            endpoint = params.getOptional("endpoint", classOf[String])
            )
        )

}
