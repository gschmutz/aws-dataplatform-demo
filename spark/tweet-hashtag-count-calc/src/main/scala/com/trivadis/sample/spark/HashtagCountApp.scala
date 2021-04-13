package com.trivadis.sample.spark

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions.col
import io.delta.tables._
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OParser

case class Config(year: String = null,
                  month: String = null,
                  day: String = null,
                  hour: String = null,
                  s3aEndpoint: String = "http://minio:9000",
                  s3aAccessKey: String = "V42FCGRVMK24JJ8DHUYG",
                  s3aSecretKey: String = "bKhWxVF3kQoLY9kFmt91l+tDrEoZjqnWXzY9Eza")

object HashtagCountApp extends App {

  println("hello HashtagCountPerHourCalculation")
  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("hashtagCountApp"),
      head("hashtagCountApp", "1.0"),
      opt[String]('y', "year")
        .required()
        .action { (x, c) => c.copy(year = x) }
        .text("year is the year it should run on"),
      opt[String]('m', "month")
        .required()
        .action { (x, c) => c.copy(month = x) }
        .text("month is the month it should run on"),
      opt[String]('d', "day")
        .required()
        .action { (x, c) => c.copy(day = x) }
        .text("day is the day it should run on"),
      opt[String]('h', "hour")
        .optional()
        .action { (x, c) => c.copy(hour = x) }
        .text("hour is the hour it should run on"),
      opt[String]('e', "s3aEndpoint")
        .optional()
        .action { (x, c) => c.copy(s3aEndpoint = x) }
        .text("s3aEndpoint is the S3a Endpoint which should be used to connect to S3"),
      opt[String]('e', "s3aAccessKey")
        .optional()
        .action { (x, c) => c.copy(s3aAccessKey = x) }
        .text("s3AccessKey is the S3a AccessKey which should be used to connect to S3"),
      opt[String]('e', "s3aSecretKey")
        .optional()
        .action { (x, c) => c.copy(s3aSecretKey = x) }
        .text("s3SecretKey is the S3a SecretKey which should be used to connect ot S3")
    )
  }
  // parser.parse returns Option[C]
  OParser.parse(parser1, args, Config()) match {
    case Some(config) =>
      // do stuff
      println("year==" + config.year)
      println("month=" + config.month)
      println("day=" + config.day)
      println("hour=" + config.hour)
      println("s3aEndpoint=" + config.s3aEndpoint)
      println("s3AccessKey=" + config.s3aAccessKey)

      // digital data object input
      val tweetRawPath = "s3a://tweet-bucket/tweets/hourly/tweet_raw_v1"

      // digital data object refined
      val hashtagCountPath = "s3a://tweet-bucket/usage-optimized/hashCountPerHour"

      val conf = new SparkConf().setAppName("HashtagCountPerHourCalculation")
      val spark = SparkSession.builder.appName("Tweet Hashtag Count per Hour Application").getOrCreate()
      spark.sparkContext.hadoopConfiguration.set("avro.mapred.ignore.inputs.without.extension", "false")
      //spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.endpoint", config.s3aEndpoint)
      spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.access.key", config.s3aAccessKey)
      spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.secret.key", config.s3aSecretKey)
      //spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.path.style.access", "true")
      spark.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

      val tweetRawDf = spark.read.format("json").load(tweetRawPath)
      val tweetFilteredRawDf = tweetRawDf.filter (s"dt = to_date('${config.year}${config.month}${config.day}','yyyyMMdd') and hr = '${config.hour}'")

      tweetFilteredRawDf.createOrReplaceTempView("tweet_raw_t")

      val hashtagCountDf = spark.sql(s"""
                SELECT to_date(from_unixtime(timestamp_ms / 1000, 'yyyyMMdd'), 'yyyyMMdd') at_date
                    , from_unixtime(timestamp_ms / 1000, 'HH') at_hour
                    , hashtag
                    , count(hashtag) noByHour
                FROM (
                    SELECT timestamp_ms
                        , id
                        , LCASE(hashtag) AS hashtag
                    FROM tweet_raw_t LATERAL VIEW explode ( entities.hashtags.text) AS hashtag
                ) GROUP BY hashtag
                        , to_date(from_unixtime(timestamp_ms / 1000, 'yyyyMMdd'), 'yyyyMMdd')
                        , from_unixtime(timestamp_ms / 1000, 'HH')
      """)

      try {
        val deltaTable = DeltaTable.forPath(spark, hashtagCountPath)

        // merge data on hashtag, at_date and at_hour
        deltaTable.alias("oldData")
          .merge(hashtagCountDf.alias("newData"),
            "oldData.hashtag = newData.hashtag AND oldData.at_date = newData.at_date AND oldData.at_hour = newData.at_hour")
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()
      } catch {
        case ae: AnalysisException =>
          hashtagCountDf.write
                .format("delta")
                .partitionBy("at_date", "at_hour")
                .save(hashtagCountPath);
      }

      val deltaTable = DeltaTable.forPath(spark, hashtagCountPath)
      deltaTable.generate("symlink_format_manifest")
    case _ =>
      throw new RuntimeException("Invalid set of parameters have been passed!")
  }
}
