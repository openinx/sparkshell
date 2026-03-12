name := "SparkShell"

version := "0.1.0"

scalaVersion := sys.env.getOrElse("SCALA_VERSION", "2.13.15")

// Read Delta configuration from environment
val deltaVersion = sys.env.getOrElse("DELTA_VERSION", "4.0.0")
val deltaUseLocal = sys.env.getOrElse("DELTA_USE_LOCAL", "false").toBoolean
val deltaSparkVersion = sys.env.getOrElse("DELTA_SPARK_VERSION", "")
// Delta CrossSparkVersions publishes delta-spark_4.0_2.13, delta-spark_4.1_2.13, etc.
val deltaArtifactSuffix =
  if (deltaSparkVersion.startsWith("4.2")) Some("4.2")
  else if (deltaSparkVersion.startsWith("4.1")) Some("4.1")
  else if (deltaSparkVersion.startsWith("4.0")) Some("4.0")
  else None
val deltaSparkModule = deltaArtifactSuffix.map(s => s"delta-spark_" + s).getOrElse("delta-spark")
val deltaIcebergModule = deltaArtifactSuffix.map(s => s"delta-iceberg_" + s).getOrElse("delta-iceberg")
val deltaSupportsIceberg = !deltaSparkVersion.startsWith("4.1") && !deltaSparkVersion.startsWith("4.2")

// Spark version for the spark-sql dependency. Must match the Delta artifact's target Spark.
val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "4.0.0")

// Read Unity Catalog configuration from environment
// UC_USE_LOCAL=true: use UC from Maven Local (requires building UC first: build/sbt publishLocal)
//    UC main branch is 0.5.0-SNAPSHOT; 0.3.0-SNAPSHOT no longer exists in the repo.
// UC_USE_LOCAL=false (default): use UC from Maven Central (released 0.3.1)
val ucUseLocal = sys.env.getOrElse("UC_USE_LOCAL", "false").toBoolean
val ucVersion = if (ucUseLocal) "0.5.0-SNAPSHOT" else "0.3.1"

// When using local Delta or UC: resolve from Maven local. Use maven.repo.local when set
// (e.g. by SparkShell for a temp repo under work_dir/m2_repo) so the build is reproducible.
val needsMavenLocal = deltaUseLocal || ucUseLocal
val localMavenRepo =
  sys.props.get("maven.repo.local").fold(Resolver.mavenLocal)(p =>
    MavenRepository("local", "file://" + new java.io.File(p).getAbsolutePath)
  )
resolvers := resolvers.value ++ (if (needsMavenLocal) Seq(localMavenRepo) else Seq.empty)

// Main class for easy running
Compile / mainClass := Some("com.sparkshell.SparkShellServer")
assembly / mainClass := Some("com.sparkshell.SparkShellServer")
assembly / assemblyJarName := "sparkshell.jar"

// Assembly merge strategy
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _ => MergeStrategy.concat
      case "versions" :: _ => MergeStrategy.first
      case _ => MergeStrategy.discard
    }
  case "reference.conf" => MergeStrategy.concat
  case "module-info.class" => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "commons", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", "hadoop", xs @ _*) => MergeStrategy.first
  case PathList("com", "amazonaws", xs @ _*) => MergeStrategy.first
  case PathList("com", "google", xs @ _*) => MergeStrategy.first
  case x if x.endsWith(".proto") => MergeStrategy.first
  case x if x.endsWith(".properties") => MergeStrategy.concat
  case _ => MergeStrategy.first
}

// JVM options for Java 9+ compatibility with Spark
fork := true
run / fork := true
run / connectInput := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false"
)

libraryDependencies ++= Seq(
  // Spark — version derived from DELTA_SPARK_VERSION or overridden via SPARK_VERSION
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Delta Lake - version configurable via DELTA_VERSION environment variable
  "io.delta" %% deltaSparkModule % deltaVersion,

  // Unity Catalog
  // UC_USE_LOCAL=true: use 0.5.0-SNAPSHOT from ~/.m2 (build UC with publishLocal first)
  // UC_USE_LOCAL=false: use Maven Central 0.3.1
  "io.unitycatalog" % "unitycatalog-spark_2.13" % ucVersion,

  // Cloud Storage Support (S3, ADLS)
  // Hadoop version must match Spark's transitive hadoop-common to avoid
  // binary incompatibilities (e.g. VectoredReadUtils API changes between 3.4.x).
  // Spark 4.0.x → Hadoop 3.4.0, Spark 4.1.x → Hadoop 3.4.1+
  "org.apache.hadoop" % "hadoop-aws" % sys.env.getOrElse("HADOOP_VERSION", "3.4.1"),
  "org.apache.hadoop" % "hadoop-azure" % sys.env.getOrElse("HADOOP_VERSION", "3.4.1"),
  // "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.22",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.367",

  // REST API
  "com.sparkjava" % "spark-core" % "2.9.4",
  "com.google.code.gson" % "gson" % "2.10.1",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
) ++ {
  if (deltaSupportsIceberg) {
    Seq("io.delta" %% deltaIcebergModule % deltaVersion)
  } else {
    Seq.empty
  }
}
