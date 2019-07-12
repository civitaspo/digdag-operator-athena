package pro.civitaspo.digdag.plugin.athena.query


import java.nio.charset.StandardCharsets.UTF_8

import com.amazonaws.services.athena.model.{QueryExecution, QueryExecutionState}
import com.amazonaws.services.athena.model.QueryExecutionState.{CANCELLED, FAILED, SUCCEEDED}
import com.amazonaws.services.s3.AmazonS3URI
import com.google.common.base.Optional
import com.google.common.collect.ImmutableList
import io.digdag.client.config.{Config, ConfigKey}
import io.digdag.spi.{OperatorContext, TaskResult, TemplateEngine}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.AbstractAthenaOperator

import scala.util.{Failure, Random, Success, Try}
import scala.util.hashing.MurmurHash3

class AthenaQueryOperator(operatorName: String,
                          context: OperatorContext,
                          systemConfig: Config,
                          templateEngine: TemplateEngine)
    extends AbstractAthenaOperator(operatorName, context, systemConfig, templateEngine)
{
    case class LastQuery(
        id: String,
        database: Option[String] = None,
        query: String,
        output: String,
        scanBytes: Option[Long] = None,
        execMillis: Option[Long] = None,
        state: QueryExecutionState,
        stateChangeReason: Option[String] = None,
        submittedAt: Option[Long] = None,
        completedAt: Option[Long] = None
    )

    object LastQuery
    {

        def apply(qe: QueryExecution): LastQuery =
        {
            new LastQuery(
                id = qe.getQueryExecutionId,
                database = Try(Option(qe.getQueryExecutionContext.getDatabase)).getOrElse(None),
                query = qe.getQuery,
                output = qe.getResultConfiguration.getOutputLocation,
                scanBytes = Try(Option(qe.getStatistics.getDataScannedInBytes.toLong)).getOrElse(None),
                execMillis = Try(Option(qe.getStatistics.getEngineExecutionTimeInMillis.toLong)).getOrElse(None),
                state = QueryExecutionState.fromValue(qe.getStatus.getState),
                stateChangeReason = Try(Option(qe.getStatus.getStateChangeReason)).getOrElse(None),
                submittedAt = Try(Option(qe.getStatus.getSubmissionDateTime.getTime / 1000)).getOrElse(None),
                completedAt = Try(Option(qe.getStatus.getCompletionDateTime.getTime / 1000)).getOrElse(None)
                )
        }
    }

    protected val queryOrFile: String = params.get("_command", classOf[String])
    protected val tokenPrefix: String = params.get("token_prefix", classOf[String], "digdag-athena")
    protected val database: Optional[String] = params.getOptional("database", classOf[String])
    @deprecated protected val outputOptional: Optional[String] = params.getOptional("output", classOf[String])
    protected val timeout: DurationParam = params.get("timeout", classOf[DurationParam], DurationParam.parse("10m"))
    protected val preview: Boolean = params.get("preview", classOf[Boolean], false)

    protected lazy val query: String = {
        val t: Try[String] =
            if (queryOrFile.startsWith("s3://")) loadQueryOnS3(queryOrFile)
            else loadQueryOnLocalFileSystem(queryOrFile)

        t.getOrElse(queryOrFile)
    }

    protected def loadQueryOnS3(uriString: String): Try[String] =
    {
        val t = Try {
            val content = aws.s3.readObject(uriString)
            templateEngine.template(content, params)
        }
        t match {
            case Success(_) => logger.info("Succeeded to load the query on S3.")
            case Failure(e) => logger.warn(s"Failed to load the query on S3.: ${e.getMessage}")
        }
        t
    }

    protected def loadQueryOnLocalFileSystem(path: String): Try[String] =
    {
        val t = Try {
            val f = workspace.getFile(path)
            workspace.templateFile(templateEngine, f.getPath, UTF_8, params)
        }
        t match {
            case Success(_) => logger.debug("Succeeded to load the query on LocalFileSystem.")
            case Failure(e) => logger.debug(s"Failed to load the query on LocalFileSystem.: ${e.getMessage}")
        }
        t
    }

    protected lazy val clientRequestToken: String = {
        val queryHash: Int = MurmurHash3.bytesHash(query.getBytes(UTF_8), 0)
        val random: String = Random.alphanumeric.take(5).mkString
        s"$tokenPrefix-$sessionUuid-$queryHash-$random"
    }

    @deprecated private def showMessageIfUnsupportedOptionExists(): Unit =
    {
        if (params.getOptional("keep_metadata", classOf[Boolean]).isPresent) {
            logger.warn("`keep_metadata` option has removed, and the behaviour is the same as `keep_metadata: true`.")
        }
        if (params.getOptional("save_mode", classOf[String]).isPresent) {
            logger.warn("`save_mode` option has removed, and the behaviour is the same as `save_mode: append` .")
        }
        if (outputOptional.isPresent) {
            logger.warn("`output` option will be removed, and the current default value will be always used.")
        }
    }

    override def runTask(): TaskResult =
    {
        showMessageIfUnsupportedOptionExists()

        val execution: QueryExecution = aws.athena.runQuery(query = query,
                                                            database = Option(database.orNull),
                                                            outputLocation = Option(outputOptional.orNull),
                                                            requestToken = Option(clientRequestToken),
                                                            successStates = Seq(SUCCEEDED),
                                                            failureStates = Seq(FAILED, CANCELLED),
                                                            timeout = timeout
                                                            )
        val lastQuery: LastQuery = LastQuery(execution)

        logger.info(s"[$operatorName] Executed ${lastQuery.id} (scan: ${lastQuery.scanBytes.orNull} bytes, time: ${lastQuery.execMillis.orNull}ms)")
        val p: Config = buildLastQueryParam(lastQuery)

        val builder = TaskResult.defaultBuilder(request)
        builder.resetStoreParams(ImmutableList.of(ConfigKey.of("athena", "last_query")))
        builder.storeParams(p)
        if (preview) builder.subtaskConfig(buildPreviewSubTaskConfig(lastQuery))
        builder.build()
    }

    protected def buildLastQueryParam(lastQuery: LastQuery): Config =
    {
        val ret = cf.create()
        val lastQueryParam = ret.getNestedOrSetEmpty("athena").getNestedOrSetEmpty("last_query")

        lastQueryParam.set("id", lastQuery.id)
        lastQueryParam.set("database", lastQuery.database.getOrElse(Optional.absent()))
        lastQueryParam.set("query", lastQuery.query)
        lastQueryParam.set("output", lastQuery.output.toString)
        lastQueryParam.set("scan_bytes", lastQuery.scanBytes.getOrElse(Optional.absent()))
        lastQueryParam.set("exec_millis", lastQuery.execMillis.getOrElse(Optional.absent()))
        lastQueryParam.set("state", lastQuery.state)
        lastQueryParam.set("state_change_reason", lastQuery.stateChangeReason.getOrElse(Optional.absent()))
        lastQueryParam.set("submitted_at", lastQuery.submittedAt.getOrElse(Optional.absent()))
        lastQueryParam.set("completed_at", lastQuery.completedAt.getOrElse(Optional.absent()))

        ret
    }

    protected def buildPreviewSubTaskConfig(lastQuery: LastQuery): Config =
    {
        val subTask: Config = cf.create()
        subTask.set("_type", "athena.preview")
        subTask.set("_command", lastQuery.id)
        subTask.set("max_rows", 10)

        subTask.set("auth_method", aws.conf.authMethod)
        subTask.set("profile_name", aws.conf.profileName)
        if (aws.conf.profileFile.isPresent) subTask.set("profile_file", aws.conf.profileFile.get())
        subTask.set("use_http_proxy", aws.conf.useHttpProxy)
        subTask.set("region", aws.region)
        if (aws.conf.endpoint.isPresent) subTask.set("endpoint", aws.conf.endpoint.get())

        subTask
    }
}
