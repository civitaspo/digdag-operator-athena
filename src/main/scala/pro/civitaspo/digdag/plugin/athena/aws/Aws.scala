package pro.civitaspo.digdag.plugin.athena.aws


import com.amazonaws.{ClientConfiguration, Protocol}
import com.amazonaws.auth.{AnonymousAWSCredentials, AWSCredentials, AWSCredentialsProvider, AWSStaticCredentialsProvider, BasicAWSCredentials, BasicSessionCredentials, EC2ContainerCredentialsProviderWrapper, EnvironmentVariableCredentialsProvider, SystemPropertiesCredentialsProvider}
import com.amazonaws.auth.profile.{ProfileCredentialsProvider, ProfilesConfigFile}
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.{AwsEnvVarOverrideRegionProvider, AwsProfileRegionProvider, AwsSystemPropertyRegionProvider, InstanceMetadataRegionProvider, Regions}
import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}
import com.google.common.base.Optional
import io.digdag.client.config.ConfigException
import pro.civitaspo.digdag.plugin.athena.aws.athena.Athena
import pro.civitaspo.digdag.plugin.athena.aws.s3.S3
import pro.civitaspo.digdag.plugin.athena.aws.sts.Sts

case class Aws(conf: AwsConf)
{

    @deprecated
    def withAthena[T](f: AmazonAthena => T): T =
    {
        val athena = buildService(AmazonAthenaClientBuilder.standard())
        try f(athena)
        finally athena.shutdown()
    }

    private[aws] def buildService[S <: AwsClientBuilder[S, T], T](builder: AwsClientBuilder[S, T]): T =
    {
        configureBuilderEndpointConfiguration(builder)
            .withClientConfiguration(clientConfiguration)
            .withCredentials(credentialsProvider)
            .build()
    }

    def s3: S3 =
    {
        S3(this)
    }

    def sts: Sts =
    {
        Sts(this)
    }

    def athena: Athena =
    {
        Athena(this)
    }

    lazy val region: String = {
        conf.authMethod match {
            case "env" =>
                conf.region
                    .or(Option(new AwsEnvVarOverrideRegionProvider().getRegion)
                            .getOrElse(Regions.DEFAULT_REGION.getName))

            case "instance" =>
                conf.region
                    .or(Option(new InstanceMetadataRegionProvider().getRegion)
                            .getOrElse(Regions.DEFAULT_REGION.getName))

            case "profile" =>
                conf.region
                    .or(Option(new AwsProfileRegionProvider().getRegion)
                            .getOrElse(Regions.DEFAULT_REGION.getName))

            case "properties" =>
                conf.region
                    .or(Option(new AwsSystemPropertyRegionProvider().getRegion)
                            .getOrElse(Regions.DEFAULT_REGION.getName))

            case "basic" | "anonymous" | "session" => conf.region.or(Regions.DEFAULT_REGION.getName)
            case _                                 => conf.region.or(Regions.DEFAULT_REGION.getName)
        }
    }

    private def configureBuilderEndpointConfiguration[S <: AwsClientBuilder[S, T], T](builder: AwsClientBuilder[S, T]): AwsClientBuilder[S, T] =
    {
        if (conf.endpoint.isPresent) {
            val ec = new EndpointConfiguration(conf.endpoint.get(), region)
            builder.setEndpointConfiguration(ec)
        }
        else {
            builder.setRegion(region)
        }
        builder
    }

    private def credentialsProvider: AWSCredentialsProvider =
    {
        if (!conf.roleArn.isPresent) return standardCredentialsProvider
        assumeRoleCredentialsProvider(standardCredentialsProvider)
    }

    private def standardCredentialsProvider: AWSCredentialsProvider =
    {
        conf.authMethod match {
            case "basic"      => basicAuthMethodAWSCredentialsProvider
            case "env"        => envAuthMethodAWSCredentialsProvider
            case "instance"   => instanceAuthMethodAWSCredentialsProvider
            case "profile"    => profileAuthMethodAWSCredentialsProvider
            case "properties" => propertiesAuthMethodAWSCredentialsProvider
            case "anonymous"  => anonymousAuthMethodAWSCredentialsProvider
            case "session"    => sessionAuthMethodAWSCredentialsProvider
            case _            =>
                throw new ConfigException(
                    s"""auth_method: "${conf.authMethod}" is not supported. available `auth_method`s are "basic", "env", "instance", "profile", "properties", "anonymous", or "session"."""
                    )
        }
    }

    private def assumeRoleCredentialsProvider(credentialsProviderToAssumeRole: AWSCredentialsProvider): AWSCredentialsProvider =
    {
        val aws = Aws(this.conf.copy(roleArn = Optional.absent()))
        val cred: BasicSessionCredentials = aws.sts.assumeRole(roleArn = conf.roleArn.get(),
                                                               roleSessionName = conf.roleSessionName,
                                                               durationSeconds = conf.assumeRoleTimeoutDuration.getDuration.getSeconds.toInt)
        new AWSStaticCredentialsProvider(cred)
    }

    private def basicAuthMethodAWSCredentialsProvider: AWSCredentialsProvider =
    {
        if (!conf.accessKeyId.isPresent) throw new ConfigException(s"""`access_key_id` must be set when `auth_method` is "${conf.authMethod}".""")
        if (!conf.secretAccessKey.isPresent) throw new ConfigException(s"""`secret_access_key` must be set when `auth_method` is "${conf.authMethod}".""")
        val credentials: AWSCredentials = new BasicAWSCredentials(conf.accessKeyId.get(), conf.secretAccessKey.get())
        new AWSStaticCredentialsProvider(credentials)
    }

    private def envAuthMethodAWSCredentialsProvider: AWSCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodEnv) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        new EnvironmentVariableCredentialsProvider
    }

    private def instanceAuthMethodAWSCredentialsProvider: AWSCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodInstance) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        // NOTE: combination of InstanceProfileCredentialsProvider and ContainerCredentialsProvider
        new EC2ContainerCredentialsProviderWrapper
    }

    private def profileAuthMethodAWSCredentialsProvider: AWSCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodProfile) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        if (!conf.profileFile.isPresent) return new ProfileCredentialsProvider(conf.profileName)
        val pf: ProfilesConfigFile = new ProfilesConfigFile(conf.profileFile.get())
        new ProfileCredentialsProvider(pf, conf.profileName)
    }

    private def propertiesAuthMethodAWSCredentialsProvider: AWSCredentialsProvider =
    {
        if (!conf.isAllowedAuthMethodProperties) throw new ConfigException(s"""auth_method: "${conf.authMethod}" is not allowed.""")
        new SystemPropertiesCredentialsProvider()
    }

    private def anonymousAuthMethodAWSCredentialsProvider: AWSCredentialsProvider =
    {
        val credentials: AWSCredentials = new AnonymousAWSCredentials
        new AWSStaticCredentialsProvider(credentials)
    }

    private def sessionAuthMethodAWSCredentialsProvider: AWSCredentialsProvider =
    {
        if (!conf.accessKeyId.isPresent) throw new ConfigException(s"""`access_key_id` must be set when `auth_method` is "${conf.authMethod}".""")
        if (!conf.secretAccessKey.isPresent) throw new ConfigException(s"""`secret_access_key` must be set when `auth_method` is "${conf.authMethod}".""")
        if (!conf.sessionToken.isPresent) throw new ConfigException(s"""`session_token` must be set when `auth_method` is "${conf.authMethod}".""")
        val credentials: AWSCredentials = new BasicSessionCredentials(conf.accessKeyId.get(), conf.secretAccessKey.get(), conf.sessionToken.get())
        new AWSStaticCredentialsProvider(credentials)
    }

    private def clientConfiguration: ClientConfiguration =
    {
        if (!conf.useHttpProxy) return new ClientConfiguration()

        val host: String = conf.httpProxy.getSecret("host")
        val port: Optional[String] = conf.httpProxy.getSecretOptional("port")
        val protocol: Protocol = conf.httpProxy.getSecretOptional("scheme").or("https") match {
            case "http"  => Protocol.HTTP
            case "https" => Protocol.HTTPS
            case _       => throw new ConfigException(s"""`athena.http_proxy.scheme` must be "http" or "https".""")
        }
        val user: Optional[String] = conf.httpProxy.getSecretOptional("user")
        val password: Optional[String] = conf.httpProxy.getSecretOptional("password")

        val cc = new ClientConfiguration()
            .withProxyHost(host)
            .withProtocol(protocol)

        if (port.isPresent) cc.setProxyPort(port.get().toInt)
        if (user.isPresent) cc.setProxyUsername(user.get())
        if (password.isPresent) cc.setProxyPassword(password.get())

        cc
    }

}
