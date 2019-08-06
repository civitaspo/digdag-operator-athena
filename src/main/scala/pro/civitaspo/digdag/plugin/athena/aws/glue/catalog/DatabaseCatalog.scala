package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import com.amazonaws.services.glue.model.{Database, GetDatabaseRequest, GetDatabasesRequest}
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue

import scala.jdk.CollectionConverters._
import scala.util.Try


case class DatabaseCatalog(glue: Glue)
{

    def describe(catalogIdOption: Option[String],
                 database: String): Database =
    {
        val req = new GetDatabaseRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setName(database)
        glue.withGlue(_.getDatabase(req)).getDatabase
    }

    def exists(catalogIdOption: Option[String],
               database: String): Boolean =
    {
        Try(describe(catalogIdOption, database)).isSuccess
    }

    def list(catalogIdOption: Option[String],
             limit: Option[Int] = None): Seq[Database] =
    {
        val req = new GetDatabasesRequest()
        catalogIdOption.foreach(req.setCatalogId)
        limit.foreach(l => req.setMaxResults(l))

        def recursiveGetDatabases(nextToken: Option[String] = None,
                                  lastDatabases: Seq[Database] = Seq()): Seq[Database] =
        {
            nextToken.foreach(req.setNextToken)
            val results = glue.withGlue(_.getDatabases(req))
            val databases = lastDatabases ++ results.getDatabaseList.asScala.toSeq
            limit.foreach { i =>
                if (databases.length >= i) return databases.slice(0, i)
            }
            Option(results.getNextToken) match {
                case Some(nt) => recursiveGetDatabases(nextToken = Option(nt), lastDatabases = databases)
                case None     => databases
            }
        }

        recursiveGetDatabases()
    }

}
