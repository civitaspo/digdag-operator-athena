package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import com.amazonaws.services.glue.model.{Database, GetDatabaseRequest}
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue

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

}
