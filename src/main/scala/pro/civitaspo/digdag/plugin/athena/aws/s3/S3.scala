package pro.civitaspo.digdag.plugin.athena.aws.s3


import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import pro.civitaspo.digdag.plugin.athena.aws.{Aws, AwsService}

import scala.collection.JavaConverters._


case class S3(aws: Aws)
    extends AwsService(aws)
{
    def withS3[A](f: AmazonS3 => A): A =
    {
        val s3 = aws.buildService(AmazonS3ClientBuilder.standard())
        try f(s3)
        finally s3.shutdown()
    }

    def readObject(location: String): String =
    {
        readObject(uri = AmazonS3UriWrapper(location))
    }

    def readObject(uri: AmazonS3URI): String =
    {
        readObject(bucket = uri.getBucket, key = uri.getKey)
    }

    def readObject(bucket: String,
                   key: String): String =
    {
        withS3(_.getObjectAsString(bucket, key))
    }

    def ls(location: String): Seq[AmazonS3URI] =
    {
        ls(uri = AmazonS3UriWrapper(location))
    }

    def ls(uri: AmazonS3URI): Seq[AmazonS3URI] =
    {
        ls(bucket = uri.getBucket, prefix = uri.getKey)
    }

    def ls(bucket: String,
           prefix: String): Seq[AmazonS3URI] =
    {
        withS3(_.listObjectsV2(bucket, prefix)).getObjectSummaries.asScala.map(_.getKey).map(k => AmazonS3UriWrapper(s"s3://$bucket/$k"))
    }

    def rm(location: String): Unit =
    {
        rm(uri = AmazonS3UriWrapper(location))
    }

    def rm(uri: AmazonS3URI): Unit =
    {
        rm(bucket = uri.getBucket, key = uri.getKey)
    }

    def rm(bucket: String,
           key: String): Unit =
    {
        withS3(_.deleteObject(bucket, key))
    }

    def rm_r(location: String): Seq[AmazonS3URI] =
    {
        rm_r(uri = AmazonS3UriWrapper(location))
    }

    def rm_r(uri: AmazonS3URI): Seq[AmazonS3URI] =
    {
        rm_r(bucket = uri.getBucket, prefix = uri.getKey)
    }

    def rm_r(bucket: String,
             prefix: String): Seq[AmazonS3URI] =
    {
        ls(bucket = bucket, prefix = prefix).map { uri =>
            rm(uri = uri)
            uri
        }
    }
}
