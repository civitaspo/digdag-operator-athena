package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import com.amazonaws.services.glue.model.{Column, CreatePartitionRequest, DeletePartitionRequest, GetPartitionRequest, Partition, PartitionInput, StorageDescriptor, UpdatePartitionRequest}
import io.digdag.client.config.ConfigException
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue

import scala.collection.JavaConverters._
import scala.util.Try


case class PartitionCatalog(glue: Glue)
{
    def add(catalogIdOption: Option[String] = None,
            database: String,
            table: String,
            partitionKv: Map[String, String],
            locationOption: Option[String] = None): Unit =
    {
        val pi = newPartitionInput(catalogIdOption,
                                   database,
                                   table,
                                   partitionKv,
                                   locationOption)

        val req = new CreatePartitionRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)
        req.setPartitionInput(pi)

        glue.withGlue(_.createPartition(req))
    }

    def update(catalogIdOption: Option[String] = None,
               database: String,
               table: String,
               partitionKv: Map[String, String],
               locationOption: Option[String] = None): Unit =
    {
        val pi = newPartitionInput(catalogIdOption,
                                   database,
                                   table,
                                   partitionKv,
                                   locationOption)

        val req = new UpdatePartitionRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)
        req.setPartitionValueList(pi.getValues)
        req.setPartitionInput(pi)

        glue.withGlue(_.updatePartition(req))
    }


    def delete(catalogIdOption: Option[String] = None,
               database: String,
               table: String,
               partitionKv: Map[String, String]): Unit =
    {
        val pi = newPartitionInput(catalogIdOption,
                                   database,
                                   table,
                                   partitionKv)

        val req = new DeletePartitionRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)
        req.setPartitionValues(pi.getValues)
        glue.withGlue(_.deletePartition(req))
    }

    def describe(catalogIdOption: Option[String] = None,
                 database: String,
                 table: String,
                 partitionKv: Map[String, String]): Partition =
    {
        val pi = newPartitionInput(catalogIdOption,
                                   database,
                                   table,
                                   partitionKv)
        val req = new GetPartitionRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)
        req.setPartitionValues(pi.getValues)
        glue.withGlue(_.getPartition(req)).getPartition
    }

    def exists(catalogIdOption: Option[String] = None,
               database: String,
               table: String,
               partitionKv: Map[String, String]): Boolean =
    {
        Try(describe(catalogIdOption, database, table, partitionKv)).isSuccess
    }

    private def newPartitionInput(catalogIdOption: Option[String] = None,
                                  database: String,
                                  table: String,
                                  partitionKv: Map[String, String],
                                  locationOption: Option[String] = None): PartitionInput =
    {
        val t = glue.table.describe(catalogIdOption, database, table)
        val sd = t.getStorageDescriptor
        val pVals: Seq[String] = t.getPartitionKeys.asScala.map { c =>
            partitionKv.getOrElse(c.getName, throw new ConfigException(
                s"Table[$database.$table] has a column${c.toString} as a partition," +
                    s" but partitionKv ${partitionKv.toString()} does not have this."
                ))
        }
        val location: String = locationOption.getOrElse {
            val l = t.getStorageDescriptor.getLocation
            val sb = StringBuilder.newBuilder
            sb.append(l)
            if (!l.endsWith("/")) sb.append("/")
            t.getPartitionKeys.asScala.zipWithIndex.foreach {
                case (c: Column, i: Int) => sb.append(c.getName + "=" + pVals(i) + "/")
            }
            sb.result()
        }
        new PartitionInput()
            .withParameters(t.getParameters)
            .withValues(pVals: _*)
            .withStorageDescriptor(
                new StorageDescriptor()
                    .withBucketColumns(sd.getBucketColumns)
                    .withColumns(sd.getColumns)
                    .withCompressed(sd.getCompressed)
                    .withInputFormat(sd.getInputFormat)
                    .withOutputFormat(sd.getOutputFormat)
                    .withNumberOfBuckets(sd.getNumberOfBuckets)
                    .withParameters(sd.getParameters)
                    .withSerdeInfo(sd.getSerdeInfo)
                    .withSkewedInfo(sd.getSkewedInfo)
                    .withSortColumns(sd.getSortColumns)
                    .withStoredAsSubDirectories(sd.getStoredAsSubDirectories)
                    .withLocation(location))
    }
}
