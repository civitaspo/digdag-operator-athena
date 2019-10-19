package pro.civitaspo.digdag.plugin.athena.aws


import com.google.common.base.Optional
import io.digdag.spi.SecretProvider
import io.digdag.util.DurationParam

case class AwsConf(
    isAllowedAuthMethodEnv: Boolean,
    isAllowedAuthMethodInstance: Boolean,
    isAllowedAuthMethodProfile: Boolean,
    isAllowedAuthMethodProperties: Boolean,
    isAllowedAuthMethodWebIdentityToken: Boolean,
    assumeRoleTimeoutDuration: DurationParam,
    accessKeyId: Optional[String],
    secretAccessKey: Optional[String],
    sessionToken: Optional[String],
    roleArn: Optional[String],
    roleSessionName: String,
    httpProxy: SecretProvider,
    authMethod: String,
    profileName: String,
    profileFile: Optional[String],
    defaultWebIdentityTokenFile: Optional[String],
    webIdentityTokenFile: Optional[String],
    defaultWebIdentityRoleArn: Optional[String],
    webIdentityRoleArn: Optional[String],
    useHttpProxy: Boolean,
    region: Optional[String],
    endpoint: Optional[String]
)
