package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import com.amazonaws.services.glue.model.{DeleteTableRequest, GetTableRequest, Table}
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue

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

}
