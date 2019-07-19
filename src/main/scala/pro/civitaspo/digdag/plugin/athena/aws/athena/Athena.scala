package pro.civitaspo.digdag.plugin.athena.aws.athena


import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}
import com.amazonaws.services.athena.model.{GetQueryExecutionRequest, GetQueryResultsRequest, QueryExecution, QueryExecutionContext, QueryExecutionState, ResultConfiguration, ResultSet, StartQueryExecutionRequest}
import io.digdag.util.DurationParam
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}


case class Athena(aws: Aws)
    extends AwsService(aws)
{
    val DEFAULT_WORKGROUP = "primary"
    lazy val DEFAULT_OUTPUT_LOCATION: String = {
        val accountId = aws.sts.getCallerIdentityAccountId
        s"s3://aws-athena-query-results-$accountId-${aws.region}/"
    }


    def withAthena[A](f: AmazonAthena => A): A =
    {
        val athena = aws.buildService(AmazonAthenaClientBuilder.standard())
        try f(athena)
        finally athena.shutdown()
    }

    def startQueryExecution(query: String,
                            database: Option[String] = None,
                            workGroup: Option[String] = None,
                            outputLocation: Option[String] = None,
                            requestToken: Option[String] = None): String =
    {
        val req = new StartQueryExecutionRequest()
        req.setQueryString(query)
        database.foreach(db => req.setQueryExecutionContext(new QueryExecutionContext().withDatabase(db)))
        req.setWorkGroup(workGroup.getOrElse(DEFAULT_WORKGROUP))
        // TODO: overwrite by workgroup configurations if workgroup is not "primary".
        req.setResultConfiguration(new ResultConfiguration().withOutputLocation(outputLocation.getOrElse(DEFAULT_OUTPUT_LOCATION)))
        requestToken.foreach(req.setClientRequestToken)

        withAthena(_.startQueryExecution(req)).getQueryExecutionId
    }

    def getQueryExecution(executionId: String): QueryExecution =
    {
        withAthena(_.getQueryExecution(new GetQueryExecutionRequest().withQueryExecutionId(executionId))).getQueryExecution
    }

    def waitQueryExecution(executionId: String,
                           successStates: Seq[QueryExecutionState],
                           failureStates: Seq[QueryExecutionState],
                           timeout: DurationParam): Unit =
    {
        val waiter = AthenaQueryWaiter(athena = this,
                                       successStats = successStates,
                                       failureStats = failureStates,
                                       timeout = timeout)
        waiter.wait(executionId)
    }

    def runQuery(query: String,
                 database: Option[String] = None,
                 workGroup: Option[String] = None,
                 outputLocation: Option[String] = None,
                 requestToken: Option[String] = None,
                 successStates: Seq[QueryExecutionState],
                 failureStates: Seq[QueryExecutionState],
                 timeout: DurationParam): QueryExecution =
    {
        val executionId: String = startQueryExecution(query = query,
                                                      database = database,
                                                      workGroup = workGroup,
                                                      outputLocation = outputLocation,
                                                      requestToken = requestToken)

        waitQueryExecution(executionId = executionId,
                           successStates = successStates,
                           failureStates = failureStates,
                           timeout = timeout)

        getQueryExecution(executionId = executionId)
    }

    def preview(executionId: String,
                limit: Int): ResultSet =
    {
        def requestRecursive(nextToken: Option[String] = None): ResultSet =
        {
            val req: GetQueryResultsRequest = new GetQueryResultsRequest()
                .withQueryExecutionId(executionId)
                .withMaxResults(limit)

            nextToken.foreach(req.setNextToken)

            val res = withAthena(_.getQueryResults(req))
            val rs = res.getResultSet.clone()

            Option(res.getNextToken).foreach { token =>
                val next = requestRecursive(Option(token))
                val rows = rs.getRows
                rows.addAll(next.getRows)
                rs.setRows(rows)
            }

            rs
        }

        requestRecursive()
    }

}
