package pro.civitaspo.digdag.plugin.athena.aws.glue.catalog


import com.amazonaws.services.glue.model.{BatchCreatePartitionRequest, BatchDeletePartitionRequest, Column, CreatePartitionRequest, DeletePartitionRequest, GetPartitionRequest, GetPartitionsRequest, Partition, PartitionError, PartitionInput, PartitionValueList, StorageDescriptor, UpdatePartitionRequest}
import io.digdag.client.config.ConfigException
import pro.civitaspo.digdag.plugin.athena.aws.glue.Glue

import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.chaining._


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
        val pVals: Seq[String] = t.getPartitionKeys.asScala.toSeq.map { c =>
            partitionKv.getOrElse(c.getName, throw new ConfigException(
                s"Table[$database.$table] has a column${c.toString} as a partition," +
                    s" but partitionKv {${partitionKv.mkString(",")}} does not have this."
                ))
        }
        val location: String = locationOption.getOrElse {
            val l = Option(t.getStorageDescriptor.getLocation).getOrElse {
                throw new IllegalStateException(s"The location of '$database.$table' is null.")
            }
            val sb = new StringBuilder()
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

    def generateLocation(catalogIdOption: Option[String] = None,
                         database: String,
                         table: String,
                         partitionKv: Map[String, String]): String =
    {
        val pi = newPartitionInput(catalogIdOption,
                                   database,
                                   table,
                                   partitionKv)
        pi.getStorageDescriptor.getLocation
    }

    def batchGet(catalogIdOption: Option[String],
                 database: String,
                 table: String,
                 partitionFilter: Option[String] = None): Seq[Partition] =
    {
        val req = new GetPartitionsRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)
        partitionFilter.foreach(req.setExpression)

        def getAllPartitions(result: Seq[Partition] = Seq(),
                             nextToken: Option[String] = None): Seq[Partition] =
        {
            nextToken.foreach(req.setNextToken)
            glue.withGlue(_.getPartitions(req)).pipe { r =>
                Option(r.getNextToken) match {
                    case Some(token) =>
                        getAllPartitions(result = result ++ r.getPartitions.asScala, nextToken = Option(token))
                    case None        => result ++ r.getPartitions.asScala
                }
            }
        }

        getAllPartitions()
    }

    def batchGetWith(catalogIdOption: Option[String],
                     database: String,
                     table: String,
                     partitionFilter: Option[String])
                    (f: Partition => Unit): Unit =
    {
        val req = new GetPartitionsRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)
        partitionFilter.foreach(req.setExpression)

        def getPartitions(nextToken: Option[String] = None): Unit =
        {
            nextToken.foreach(req.setNextToken)
            val result = glue.withGlue(_.getPartitions(req))
            result.getPartitions.asScala.foreach(f)
            val token = Option(result.getNextToken)
            if (token.isDefined) getPartitions(token)
        }

        getPartitions()
    }

    def batchCreate(catalogIdOption: Option[String] = None,
                    database: String,
                    table: String,
                    partitions: Seq[Partition]): Unit =
    {
        val req = new BatchCreatePartitionRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)

        val pis: Seq[PartitionInput] = partitions.map { p =>
            new PartitionInput().tap { pi =>
                pi.setLastAccessTime(p.getLastAccessTime)
                pi.setLastAnalyzedTime(p.getLastAnalyzedTime)
                pi.setParameters(p.getParameters)
                pi.setStorageDescriptor(p.getStorageDescriptor)
                pi.setValues(p.getValues)
            }
        }

        req.setPartitionInputList(pis.asJava)

        val result = glue.withGlue(_.batchCreatePartition(req))
        if (!result.getErrors.isEmpty) {
            val errors: Seq[PartitionError] = result.getErrors.asScala.toSeq
            val errorCodeSummary: Map[String, Int] = errors.foldLeft(Map[String, Int]()) { (summary,
                                                                                            e) =>
                val code: String = e.getErrorDetail.getErrorCode
                val sum: Int = summary.getOrElse(code, 0)
                summary.updated(code, sum + 1)
            }
            throw new IllegalStateException(s"error code summary: ${errorCodeSummary.mkString(", ")}, errors: ${errors.map(_.toString).mkString(", ")}")
        }
    }

    def batchDelete(catalogIdOption: Option[String] = None,
                    database: String,
                    table: String,
                    partitions: Seq[Partition]): Unit =
    {
        val req = new BatchDeletePartitionRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(database)
        req.setTableName(table)
        req.setPartitionsToDelete(partitions.map { p =>
            val pvl = new PartitionValueList()
            pvl.setValues(p.getValues)
            pvl
        }.asJava)

        val result = glue.withGlue(_.batchDeletePartition(req))
        if (!result.getErrors.isEmpty) {
            val errors: Seq[PartitionError] = result.getErrors.asScala.toSeq
            val errorCodeSummary: Map[String, Int] = errors.foldLeft(Map[String, Int]()) { (summary,
                                                                                            e) =>
                val code: String = e.getErrorDetail.getErrorCode
                val sum: Int = summary.getOrElse(code, 0)
                summary.updated(code, sum + 1)
            }
            throw new IllegalStateException(s"error code summary: ${errorCodeSummary.mkString(", ")}, errors: ${errors.map(_.toString).mkString(", ")}")
        }
    }

    def update(catalogIdOption: Option[String],
               partition: Partition): Unit =
    {
        val req = new UpdatePartitionRequest()
        catalogIdOption.foreach(req.setCatalogId)
        req.setDatabaseName(partition.getDatabaseName)
        req.setTableName(partition.getTableName)
        req.setPartitionValueList(partition.getValues)
        req.setPartitionInput(new PartitionInput()
                              .withLastAccessTime(partition.getLastAccessTime)
                              .withLastAnalyzedTime(partition.getLastAnalyzedTime)
                              .withParameters(partition.getParameters)
                              .withStorageDescriptor(partition.getStorageDescriptor)
                              .withValues(partition.getValues))
        glue.withGlue(_.updatePartition(req))
    }
}
