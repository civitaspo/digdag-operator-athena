package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import com.amazonaws.services.glue.model.{DeleteTableRequest, GetTableRequest, GetTablesRequest, Table}
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue

import scala.jdk.CollectionConverters._
import scala.util.Try


case class TableCatalog(glue: Glue)
{
    def describe(catalogIdOption: Option[String],
                 database: String,
                 table: String): Table =
    {
        val req = new GetTableRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setName(table)
        glue.withGlue(_.getTable(req)).getTable
    }

    def isPartitioned(catalogIdOption: Option[String],
                      database: String,
                      table: String): Boolean =
    {
        !describe(catalogIdOption, database, table).getPartitionKeys.isEmpty
    }

    def exists(catalogIdOption: Option[String],
               database: String,
               table: String): Boolean =
    {
        Try(describe(catalogIdOption, database, table)).isSuccess
    }

    def delete(catalogIdOption: Option[String],
               database: String,
               table: String): Unit =
    {
        val req = new DeleteTableRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setName(table)
        glue.withGlue(_.deleteTable(req))
    }

    def list(catalogIdOption: Option[String],
             database: String,
             expression: Option[String] = None,
             limit: Option[Int] = None): Seq[Table] =
    {
        val req = new GetTablesRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        expression.foreach(req.setExpression)
        limit.foreach(l => req.setMaxResults(l))

        def recursiveGetTables(nextToken: Option[String] = None): Seq[Table] =
        {
            nextToken.foreach(req.setNextToken)
            val results = glue.withGlue(_.getTables(req))
            val tables = results.getTableList.asScala.toSeq
            limit.foreach { i =>
                if (tables.length >= i) return tables.slice(0, i)
            }
            Option(results.getNextToken) match {
                case Some(nt) => tables ++ recursiveGetTables(nextToken = Option(nt))
                case None     => tables
            }
        }

        recursiveGetTables()
    }
}
