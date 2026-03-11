package com.sparkshell

import org.apache.spark.sql.SparkSession


class SparkShellServer(spark: SparkSession, port: Int) {
  private var restApi: RestApi = _

  def start(): Unit = {
    restApi = new RestApi(spark, port)
    restApi.start()

    sys.addShutdownHook {
      System.err.println("Shutting down REST server...")
      stop()
      spark.stop()
      System.err.println("Server shut down.")
    }
  }

  def stop(): Unit = {
    if (restApi != null) {
      restApi.stop()
    }
  }

  def blockUntilShutdown(): Unit = {
    // Keep the main thread alive
    try {
      Thread.currentThread().join()
    } catch {
      case _: InterruptedException =>
        System.err.println("Server interrupted, shutting down...")
    }
  }
}

object SparkShellServer {
  private val DEFAULT_PORT = 8080

  private def tryRegisterServerSidePlanningFactory(): Unit = {
    try {
      val factoryClass = Class.forName(
        "org.apache.spark.sql.delta.serverSidePlanning.IcebergRESTCatalogPlanningClientFactory")
      val singletonField = factoryClass.getField("MODULE$")
      val factoryInstance = singletonField.get(null)

      val registryClass = Class.forName(
        "org.apache.spark.sql.delta.serverSidePlanning.ServerSidePlanningClientFactory")
      val setFactory = registryClass.getMethod("setFactory", factoryClass)
      setFactory.invoke(null, factoryInstance)
      System.err.println("Registered IcebergRESTCatalogPlanningClientFactory for server-side planning")
    } catch {
      case _: ClassNotFoundException =>
        System.err.println("Server-side planning classes not found; skipping FGAC planning client registration")
      case e: Exception =>
        System.err.println(s"Warning: failed to register server-side planning client factory: ${e.getMessage}")
    }
  }

  def main(args: Array[String]): Unit = {
    // Redirect System.out to a file so Log4j's ConsoleAppender (which
    // targets stdout by default) can never block on a full pipe buffer.
    // When the parent process captures our stdout via a pipe, a slow reader
    // causes FileOutputStream.writeBytes to block in the kernel while
    // holding PrintStream's ReentrantLock — deadlocking every thread that
    // tries to print, including HTTP handler threads.
    // We keep stderr for our own diagnostic output (see RestApi.log).
    val logDir = sys.env.getOrElse("SPARKSHELL_LOG_DIR",
      System.getProperty("java.io.tmpdir"))
    val stdoutLog = new java.io.File(logDir, "sparkshell_stdout.log")
    val stdoutStream = new java.io.PrintStream(
      new java.io.BufferedOutputStream(
        new java.io.FileOutputStream(stdoutLog, true)),
      true)
    System.setOut(stdoutStream)
    System.err.println(s"[SparkShellServer] stdout redirected to ${stdoutLog.getAbsolutePath}")

    // Parse arguments: port [key1=value1 key2=value2 ...]
    val port = if (args.length > 0) args(0).toInt else DEFAULT_PORT
    val sparkConfigs = if (args.length > 1) {
      args.drop(1).map { arg =>
        val parts = arg.split("=", 2)
        if (parts.length == 2) Some((parts(0), parts(1))) else None
      }.flatten.toMap
    } else {
      Map.empty[String, String]
    }

    // Initialize Spark Session with Delta and Unity Catalog support
    val builder = SparkSession.builder()
      .appName("Spark SQL REST Server")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      // Delta Lake configurations
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      // Unity Catalog configuration (catalog type only, URI and token must be provided via spark_configs)
      // .config("spark.sql.catalog.unity", "io.unitycatalog.spark.UCSingleCatalog")
      // Cloud storage configurations (enable S3, Azure, GCS filesystems)
      .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.azure.impl", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    
    // Apply custom Spark configurations
    val builderWithConfigs = sparkConfigs.foldLeft(builder) { case (b, (key, value)) =>
      System.err.println(s"Applying custom Spark config: $key = $value")
      b.config(key, value)
    }
    
    val spark = builderWithConfigs.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    def log(msg: String): Unit = System.err.println(msg)

    // Print version information
    log("=" * 60)
    log("Runtime Configuration:")
    log(s"  Spark:           ${spark.version}")

    // Get Delta Lake version
    try {
      val deltaVersion = io.delta.VERSION
      log(s"  Delta Lake:      $deltaVersion")
    } catch {
      case _: Exception => log(s"  Delta Lake:      enabled (version unknown)")
    }

    // Check Unity Catalog
    if (sparkConfigs.contains("spark.sql.catalog.unity.uri") &&
        sparkConfigs.contains("spark.sql.catalog.unity.token")) {
      log(s"  Unity Catalog:   enabled (${sparkConfigs("spark.sql.catalog.unity.uri")})")
    } else {
      log(s"  Unity Catalog:   available (not configured)")
    }
    log("=" * 60)

    // Register server-side planning client factory for FGAC support (when available).
    tryRegisterServerSidePlanningFactory()

    // Eagerly initialize Spark internals to avoid lazy loading issues
    try {
      spark.sql("SELECT 1").collect()
      log("Spark internals pre-initialized successfully")
    } catch {
      case e: Exception =>
        log(s"Warning during Spark pre-initialization: ${e.getMessage}")
    }

    // Start REST API Server
    val server = new SparkShellServer(spark, port)
    server.start()
    server.blockUntilShutdown()
  }
}
