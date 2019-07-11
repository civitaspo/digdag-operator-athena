package pro.civitaspo.digdag.plugin.athena.aws.athena


import com.amazonaws.services.athena.{AmazonAthena, AmazonAthenaClientBuilder}
import com.amazonaws.services.athena.model.{QueryExecutionContext, ResultConfiguration, StartQueryExecutionRequest}
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}


object Athena
{
    def apply(aws: Aws): Athena =
    {
        new Athena(aws)
    }
}

class Athena(aws: Aws)
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

    def startQuery(query: String,
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

}
