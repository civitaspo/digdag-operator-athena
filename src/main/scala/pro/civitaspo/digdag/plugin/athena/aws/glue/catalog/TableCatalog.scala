package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import com.amazonaws.services.glue.model.{GetTableRequest, Table}
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

    def exists(catalogIdOption: Option[String],
               database: String,
               table: String): Boolean =
    {
        Try(describe(catalogIdOption, database, table)).isSuccess
    }

}
