lazy val root = project
  .in(file("."))
  .settings(
    name := "protonCase",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := "2.13.12",
    
    // Use Java 11 for this project (compatible with Spark 3.5.0)
    javaHome := Some(file("/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home")),
    
    // Suppress JVM warnings about illegal reflective access (Not actionable)
    fork := true,
    run / javaOptions ++= Seq(
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/java.net=ALL-UNNAMED",
      "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"  // Added for ConcurrentHashMap warnings
    ),
    
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.0",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "org.scalameta" %% "munit" % "1.0.0" % Test
    )
  )
