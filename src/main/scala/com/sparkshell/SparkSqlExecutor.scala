package com.sparkshell

import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Try, Success, Failure}

class SparkSqlExecutor(spark: SparkSession) {

  private def log(msg: String): Unit = System.err.println(msg)

  def executeSql(sqlQuery: String, outputPath: Option[String] = None): SqlResult = {
    Try {
      val df = spark.sql(sqlQuery)

      // Check if this is a DDL/DML command or a query
      if (df.schema.isEmpty) {
        // DDL/DML command (CREATE, INSERT, etc.)
        SqlResult(success = true, result = "Command executed successfully", error = None)
      } else {
        // Query that returns data
        outputPath match {
          case Some(path) =>
            // Write to Parquet and return metadata
            writeToParquet(df, path)
          case None =>
            // Format and return as string (original behavior)
            val resultString = formatDataFrame(df)
            SqlResult(success = true, result = resultString, error = None)
        }
      }
    } match {
      case Success(result) => result
      case Failure(exception) =>
        SqlResult(success = false, result = "", error = Some(exception.getMessage))
    }
  }

  private def formatDataFrame(df: DataFrame): String = {
    try {
      // First attempt: Use show() which handles complex types better than collect()
      // show() uses internal formatting that properly handles nested structs, arrays, maps, etc.
      val output = new java.io.ByteArrayOutputStream()
      val printStream = new java.io.PrintStream(output)
      Console.withOut(printStream) {
        df.show(numRows = 1000, truncate = false)
      }
      printStream.close()
      output.toString("UTF-8")
    } catch {
      case e: Exception if e.getMessage != null && e.getMessage.contains("EXPRESSION_DECODING_FAILED") =>
        // Fallback 1: Try converting to JSON, which handles all Spark types
        try {
          val jsonRows = df.toJSON.collect()
          if (jsonRows.isEmpty) {
            "Empty result set"
          } else {
            jsonRows.mkString("\n") + s"\n\nTotal rows: ${jsonRows.length}"
          }
        } catch {
          case jsonError: Exception =>
            // Fallback 2: Return schema and row count only
            try {
              val rowCount = df.count()
              s"Query executed successfully but result formatting failed.\n" +
              s"This can happen with complex nested types (struct/array/map).\n\n" +
              s"Schema:\n${df.schema.treeString}\n" +
              s"Row count: $rowCount\n\n" +
              s"Error: ${e.getMessage}"
            } catch {
              case countError: Exception =>
                // Last resort: just show we tried
                s"Query executed but result processing failed.\n" +
                s"Schema:\n${df.schema.treeString}\n" +
                s"Error: ${e.getMessage}"
            }
        }
      case e: Exception =>
        // For other exceptions, try the original collect() approach
        try {
          val rows = df.collect()
          val schema = df.schema
          
          if (rows.isEmpty) {
            "Empty result set"
          } else {
            val header = schema.fields.map(_.name).mkString(" | ")
            val separator = "-" * header.length
            val data = rows.map { row =>
              row.toSeq.map(v => if (v == null) "null" else v.toString).mkString(" | ")
            }.mkString("\n")
            
            s"$header\n$separator\n$data\n\nTotal rows: ${rows.length}"
          }
        } catch {
          case collectError: Exception =>
            s"Query executed but result formatting failed.\n" +
            s"Schema:\n${df.schema.treeString}\n" +
            s"Error: ${e.getMessage}"
        }
    }
  }

  private def writeToParquet(df: DataFrame, outputPath: String): SqlResult = {
    try {
      // Get row count before writing (cached to avoid reading twice)
      val cachedDf = df.cache()
      val rowCount = cachedDf.count()
      
      // Write to Parquet
      log(s"Writing ${rowCount} rows to Parquet at: $outputPath")
      cachedDf.write
        .mode("overwrite")  // Overwrite if exists
        .parquet(outputPath)
      
      // Unpersist cache
      cachedDf.unpersist()
      
      // List all Parquet files that were written
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(
        new java.net.URI(outputPath),
        hadoopConf
      )
      val outputPathObj = new org.apache.hadoop.fs.Path(outputPath)
      
      val files = if (fs.exists(outputPathObj)) {
        fs.listStatus(outputPathObj)
          .map(_.getPath.toString)
          .filter(_.endsWith(".parquet"))  // Only list .parquet files (skip _SUCCESS, _committed, etc.)
          .sorted
      } else {
        Array.empty[String]
      }
      
      val filesList = if (files.isEmpty) {
        "No Parquet files found (write may have failed)"
      } else {
        files.mkString("\n")
      }
      
      // Return metadata with file list
      val metadata = 
        s"""Results written to Parquet successfully!
           |
           |Output Directory: $outputPath
           |Row Count: $rowCount
           |Parquet Files: ${files.length}
           |
           |Files:
           |$filesList
           |
           |Schema:
           |${df.schema.treeString}
           |""".stripMargin
      
      SqlResult(success = true, result = metadata, error = None)
      
    } catch {
      case e: Exception =>
        SqlResult(
          success = false, 
          result = "", 
          error = Some(s"Failed to write Parquet: ${e.getMessage}")
        )
    }
  }
}

case class SqlResult(success: Boolean, result: String, error: Option[String])
