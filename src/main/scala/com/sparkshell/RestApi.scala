package com.sparkshell

import com.google.gson.Gson
import org.apache.spark.sql.SparkSession
import spark.{Request, Response, Spark}

class RestApi(sparkSession: SparkSession, port: Int) {

  private val gson = new Gson()
  private val executor = new SparkSqlExecutor(sparkSession)

  /**
   * Non-blocking log: writes to stderr to avoid contention with Log4j's
   * ConsoleAppender which holds the System.out PrintStream lock.  If the
   * stdout pipe buffer is full (common when the parent process doesn't drain
   * it fast enough), any thread calling System.out.println blocks in the
   * kernel AND holds the PrintStream ReentrantLock, deadlocking every other
   * thread that tries to print — including HTTP handler threads.
   */
  private def log(msg: String): Unit = System.err.println(msg)

  def start(): Unit = {
    // Set port
    Spark.port(port)

    // Enable CORS
    Spark.before((request, response) => {
      response.header("Access-Control-Allow-Origin", "*")
      response.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
      response.header("Access-Control-Allow-Headers", "Content-Type")
    })

    // Health check endpoint
    Spark.get("/health", (req: Request, res: Response) => {
      res.`type`("application/json")
      """{"status":"ok","message":"SparkApp server is running"}"""
    })

    // Execute SQL endpoint
    Spark.post("/sql", (req: Request, res: Response) => {
      res.`type`("application/json")

      try {
        val requestBody = req.body()
        val sqlRequest = gson.fromJson(requestBody, classOf[SqlRequestJson])

        if (sqlRequest.sql == null || sqlRequest.sql.trim.isEmpty) {
          res.status(400)
          gson.toJson(SqlResponseJson(
            success = false,
            result = null,
            error = "SQL query is required"
          ))
        } else {

          log(s"Executing SQL: ${sqlRequest.sql}")
          
          // Convert Java null to Scala Option
          val outputPathOpt = Option(sqlRequest.outputPath)
          outputPathOpt.foreach(path => log(s"Output path: $path"))
          
          val result = executor.executeSql(sqlRequest.sql, outputPathOpt)

          val response = SqlResponseJson(
            success = result.success,
            result = if (result.success) result.result else null,
            error = result.error.orNull
          )

          if (!result.success) {
            res.status(400)
          }

          gson.toJson(response)
        }
      } catch {
        case e: Exception =>
          res.status(500)
          gson.toJson(SqlResponseJson(
            success = false,
            result = null,
            error = s"Server error: ${e.getMessage}"
          ))
      }
    })

    // Info endpoint
    Spark.get("/info", (req: Request, res: Response) => {
      res.`type`("application/json")

      // Get Delta version
      val deltaVer = try {
        io.delta.VERSION
      } catch {
        case _: Exception => "unknown"
      }

      val info = ServerInfo(
        sparkVersion = sparkSession.version,
        deltaVersion = deltaVer,
        port = port.toString,
        endpoints = EndpointsInfo(
          health = "GET /health",
          execute = "POST /sql",
          info = "GET /info"
        )
      )
      gson.toJson(info)
    })

    // Wait for initialization
    Spark.awaitInitialization()
    log(s"REST API server started on port $port")
    log(s"Available endpoints:")
    log(s"  GET  http://localhost:$port/health - Health check")
    log(s"  GET  http://localhost:$port/info - Server info")
    log(s"  POST http://localhost:$port/sql - Execute SQL")
  }

  def stop(): Unit = {
    Spark.stop()
  }
}

// JSON request/response classes
case class SqlRequestJson(
  sql: String,
  outputPath: String = null  // Optional: write results to Parquet at this path (null if not provided)
)

case class SqlResponseJson(
  success: Boolean,
  result: String,
  error: String
)

case class EndpointsInfo(
  health: String,
  execute: String,
  info: String
)

case class ServerInfo(
  sparkVersion: String,
  deltaVersion: String,
  port: String,
  endpoints: EndpointsInfo
)
