package pro.civitaspo.digdag.plugin.athena.aws.glue


import com.amazonaws.services.glue.{AWSGlue, AWSGlueClientBuilder}
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}
import pro.civitaspo.digdag.plugin.athena.aws.glue.catalog.{DatabaseCatalog, PartitionCatalog, TableCatalog}


case class Glue(aws: Aws)
    extends AwsService(aws)
{
    def withGlue[A](f: AWSGlue => A): A =
    {
        val glue = aws.buildService(AWSGlueClientBuilder.standard())
        try f(glue)
        finally glue.shutdown()
    }

    // Use catalog api https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog.html
    val database: DatabaseCatalog = catalog.DatabaseCatalog(glue = this)
    val table: TableCatalog = catalog.TableCatalog(glue = this)
    val partition: PartitionCatalog = catalog.PartitionCatalog(glue = this)
}
