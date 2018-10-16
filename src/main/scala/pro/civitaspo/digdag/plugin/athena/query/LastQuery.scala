package pro.civitaspo.digdag.plugin.athena.query
import com.amazonaws.services.athena.model.{QueryExecution, QueryExecutionState}
import com.amazonaws.services.s3.AmazonS3URI
import pro.civitaspo.digdag.plugin.athena.aws.AmazonS3URI

import scala.util.Try

case class LastQuery(
  id: String,
  database: Option[String] = None,
  query: String,
  output: AmazonS3URI,
  scanBytes: Option[Long] = None,
  execMillis: Option[Long] = None,
  state: QueryExecutionState,
  stateChangeReason: Option[String] = None,
  submittedAt: Option[Long] = None,
  completedAt: Option[Long] = None
)

object LastQuery {

  def apply(qe: QueryExecution): LastQuery = {
    new LastQuery(
      id = qe.getQueryExecutionId,
      database = Try(Option(qe.getQueryExecutionContext.getDatabase)).getOrElse(None),
      query = qe.getQuery,
      output = AmazonS3URI(qe.getResultConfiguration.getOutputLocation),
      scanBytes = Try(Option(qe.getStatistics.getDataScannedInBytes.toLong)).getOrElse(None),
      execMillis = Try(Option(qe.getStatistics.getEngineExecutionTimeInMillis.toLong)).getOrElse(None),
      state = QueryExecutionState.fromValue(qe.getStatus.getState),
      stateChangeReason = Try(Option(qe.getStatus.getStateChangeReason)).getOrElse(None),
      submittedAt = Try(Option(qe.getStatus.getSubmissionDateTime.getTime / 1000)).getOrElse(None),
      completedAt = Try(Option(qe.getStatus.getCompletionDateTime.getTime / 1000)).getOrElse(None)
    )
  }
}
