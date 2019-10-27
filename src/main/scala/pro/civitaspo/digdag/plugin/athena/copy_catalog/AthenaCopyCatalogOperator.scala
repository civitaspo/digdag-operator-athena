package pro.civitaspo.digdag.plugin.athena.copy_catalog


import java.util.{List => JList}

import com.amazonaws.services.glue.model.Partition
import io.digdag.client.config.{Config, ConfigException}
import io.digdag.spi.{OperatorContext, SecretProvider, TaskResult, TemplateEngine}
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator
import pro.civitaspo.digdag.plugin.athena.aws.Aws
import pro.civitaspo.digdag.plugin.athena.util.SecretProviderChain

import scala.jdk.CollectionConverters._
import scala.util.chaining._


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
    protected val catalogId: Option[String] = Option(params.getOptional("catalog_id", classOf[String]).orNull())
    protected val saveMode: SaveMode = SaveMode(params.get("save_mode", classOf[String], "skip_if_exists"))

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
        targets.foreach { t =>
            createOrUpdateOrSkipOrErrorDatabase(t)
            createOrUpdateOrSkipOrErrorTable(t)
            createOrUpdateOrSkipOrErrorPartitions(t)
        }
        TaskResult.empty(cf)
    }

    protected def createOrUpdateOrSkipOrErrorDatabase(targetInfo: TargetInfo): Unit =
    {
        if (xAws.glue.database.exists(xCatalogId, targetInfo.database)) {
            saveMode match {
                case SaveMode.Overwrite     =>
                    logger.info(s"Overwrite the database '${targetInfo.database}'.")
                    val database = aws.glue.database.describe(catalogId, targetInfo.database)
                    xAws.glue.database.update(xCatalogId, database)
                case SaveMode.SkipIfExists  =>
                    logger.info(s"Skip processing because the database '${targetInfo.database}' already exists.")
                case SaveMode.ErrorIfExists =>
                    throw new IllegalStateException(s"'${targetInfo.database}' already exists.")
            }
        }
        else {
            val database = aws.glue.database.describe(catalogId, targetInfo.database)
            logger.info(s"Create the database: ${database.toString}")
            xAws.glue.database.create(xCatalogId, database)
        }
    }

    protected def createOrUpdateOrSkipOrErrorTable(targetInfo: TargetInfo): Unit =
    {
        if (xAws.glue.table.exists(xCatalogId, targetInfo.database, targetInfo.table)) {
            saveMode match {
                case SaveMode.Overwrite     =>
                    logger.info(s"Overwrite the table'${targetInfo.database}.${targetInfo.table}'.")
                    val table = aws.glue.table.describe(catalogId, targetInfo.database, targetInfo.table)
                    xAws.glue.table.update(xCatalogId, table)
                case SaveMode.SkipIfExists  =>
                    logger.info(s"Skip processing because the table '${targetInfo.database}.${targetInfo.table}' already exists.")
                case SaveMode.ErrorIfExists =>
                    throw new IllegalStateException(s"'${targetInfo.database}.${targetInfo.table}' already exists.")
            }
        }
        else {
            val table = aws.glue.table.describe(catalogId, targetInfo.database, targetInfo.table)
            logger.info(s"Create the table: ${table.toString}")
            xAws.glue.table.create(xCatalogId, table)
        }
    }

    protected def createOrUpdateOrSkipOrErrorPartitions(targetInfo: TargetInfo): Unit =
    {
        val xPartValuesSet: Set[JList[String]] = Set.newBuilder[JList[String]].pipe { builder =>
            xAws.glue.partition.batchGetWith(xCatalogId, targetInfo.database, targetInfo.table, targetInfo.partitionFilter) { p =>
                if (saveMode.equals(SaveMode.ErrorIfExists)) throw new IllegalStateException(s"'${targetInfo.database}.${targetInfo.table}', partition: ${p.toString} already exist.")
                builder.addOne(p.getValues)
            }
            builder.result()
        }

        val newPartsBuilder = Seq.newBuilder[Partition]
        aws.glue.partition.batchGetWith(catalogId, targetInfo.database, targetInfo.table, targetInfo.partitionFilter) { p =>
            if (!xPartValuesSet.contains(p.getValues)) {
                logger.info(s"Create a new partition: ${p.toString}")
                newPartsBuilder.addOne(p)
            }
            else {
                logger.info(s"Destination table '${targetInfo.database}.${targetInfo.table}' already has the partition: ${p.toString}")
                saveMode match {
                    case SaveMode.Overwrite    =>
                        logger.info(s"Overwrite the partition: ${p.toString}")
                        xAws.glue.partition.update(xCatalogId, p)
                    case SaveMode.SkipIfExists =>
                        logger.info(s"Skip processing because the destination table '${targetInfo.database}.${targetInfo.table}' already has the partition: ${p.toString}")
                    case _                     => throw new IllegalStateException()
                }
            }
        }

        xAws.glue.partition.batchCreate(xCatalogId, targetInfo.database, targetInfo.table, newPartsBuilder.result())
    }
}
