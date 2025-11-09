package task1_exploration

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, FileOutputStream, PrintStream, OutputStream}
import org.apache.log4j.{Logger, PropertyConfigurator}
import utils.DataLoader

/**
 * Task 1: Data Exploration
 * 
 * Usage:
 *   sbt "runMain task1_exploration.Task1Exploration [path_to_parquet_directory]"
 * 
 * If no path is provided, defaults to: ../yellow_tripdata (relative to project directory)
 * This makes the project portable - just copy protoncase and yellow_tripdata together
 * 
 * This application performs comprehensive data exploration including:
 * - Basic dataset statistics
 * - Missing values analysis
 * - Critical fields for tasks 2-4 (trip_distance, locations, total_amount, timestamps)
 * - Categorical field distributions
 * - Data quality issues
 * - Time-based flags and surcharges
 */
object Task1Exploration {
  
  // Custom OutputStream that writes to both console and file (like Unix tee command)
  class TeeOutputStream(out: OutputStream, file: OutputStream) extends OutputStream {
    override def write(b: Int): Unit = {
      out.write(b)
      file.write(b)
      file.flush()
    }
    
    override def write(b: Array[Byte]): Unit = {
      out.write(b)
      file.write(b)
      file.flush()
    }
    
    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      out.write(b, off, len)
      file.write(b, off, len)
      file.flush()
    }
    
    override def flush(): Unit = {
      out.flush()
      file.flush()
    }
    
    override def close(): Unit = {
      out.close()
      file.close()
    }
  }
  
  def main(args: Array[String]): Unit = {
    
    // Configure logging BEFORE Spark initializes to prevent duplicate logs
    // Clear any existing appenders first, then load log4j.properties
    Logger.getRootLogger().removeAllAppenders()

    val log4jProps = getClass.getClassLoader.getResource("log4j.properties")
    if (log4jProps != null) {
      PropertyConfigurator.configure(log4jProps)
    }
    
    // Set up output file in the task1_exploration directory
    val sourceDir = new File("src/main/scala/task1_exploration")
    sourceDir.mkdirs()
    val outputFile = new File(sourceDir, "Task1Exploration_output.txt")
    
    // Save original System.out
    val originalOut = System.out
    
    // Create file output stream (overwrite mode - append=false)
    val fileOutputStream = new FileOutputStream(outputFile, false)
    
    // Create TeeOutputStream that writes to both console and file
    val teeStream = new TeeOutputStream(originalOut, fileOutputStream)
    val teePrintStream = new PrintStream(teeStream, true, "UTF-8")
    
    // Redirect System.out to tee stream
    System.setOut(teePrintStream)
    
    try {
      val spark = SparkSession.builder()
        .appName("Task1Exploration")
        .master("local[*]")
        .getOrCreate()
      
      import spark.implicits._
      
      // Default to yellow_tripdata directory in the same parent directory as the project
      // This allows the project to be portable - just copy protoncase and yellow_tripdata together
      // Using DataLoader's default path logic for consistency across all tasks
      val dataPath = if (args.length > 0) args(0) else DataLoader.getDefaultDataPath()
      println(s"\nLoading data from: $dataPath\n")
      
      println("=" * 80)
      println("TASK 1: DATA EXPLORATION")
      println("=" * 80)

    // Step 1: Read data
    println("\nSTEP 1: READING DATA")
    println("-" * 80)
    
    println("\nUsing DataLoader utility to load and merge data...")
    
    // Use the reusable DataLoader utility
    val df = DataLoader.loadMergedData(spark, dataPath, verbose = true)
    
    println(s"\nSchema:")
    df.schema.fields.foreach { field =>
      println(f"  ${field.name}%-30s : ${field.dataType}")
    }
    
    val totalCount = df.count()
    println(s"\nTotal records: $totalCount")
    
    // Payment type mapping (used in multiple sections)
    val paymentTypeMapping = Map(
      0 -> "Flex Fare trip",
      1 -> "Credit card",
      2 -> "Cash",
      3 -> "No charge",
      4 -> "Dispute",
      5 -> "Unknown",
      6 -> "Voided trip"
    )
    
    // Step 2: Missing Values Analysis
    println("\n" + "=" * 80)
    println("STEP 2: MISSING VALUES ANALYSIS")
    println("=" * 80)
    println("%-30s %-15s %-10s".format("Column", "Null Count", "Null %"))
    println("-" * 80)
    df.columns.foreach { colName =>
      val nullCount = df.filter(col(colName).isNull).count()
      val nullPct = if (totalCount > 0) (nullCount.toDouble / totalCount) * 100 else 0.0
      println(f"$colName%-30s $nullCount%-15d ${nullPct}%.2f%%")
    }
    
    // Step 3: Column-Level Distribution Analysis
    println("\n" + "=" * 80)
    println("STEP 3: COLUMN-LEVEL DISTRIBUTION ANALYSIS")
    println("=" * 80)
    
    // 4.1 Dimension Variables - Group by count distributions
    val dimensionColumns = Seq("VendorID", "RatecodeID", "passenger_count", "payment_type")
    println("\n4.1 DIMENSION VARIABLES - Distribution")
    println("-" * 80)
    
    dimensionColumns.foreach { colName =>
      if (df.columns.contains(colName)) {
        println(s"\n$colName:")
        // For payment_type, show description column
        if (colName == "payment_type") {
          println("%-30s %-15s %-10s %-30s".format("Value", "Count", "Percentage", "Description"))
        } else {
          println("%-30s %-15s %-10s".format("Value", "Count", "Percentage"))
        }
        println("-" * 80)
        val counts = df.groupBy(colName)
          .agg(count("*").alias("count"))
          .withColumn("percentage", (col("count") / totalCount) * 100)
          .orderBy(desc("count"))
          .collect()
        
        counts.foreach { row =>
          val rawValue = row.get(0)
          val value = if (rawValue == null) "NULL" else rawValue.toString
          val count = row.getAs[Long]("count")
          val pct = row.getAs[Double]("percentage")
          
          if (colName == "payment_type" && rawValue != null) {
            // Apply payment type mapping
            val paymentTypeCode = rawValue match {
              case l: Long => l.toInt
              case i: Int => i
              case d: Double => d.toInt
              case _ => value.toInt
            }
            val description = paymentTypeMapping.getOrElse(paymentTypeCode, s"Unknown code ($paymentTypeCode)")
            val pctStr = f"${pct}%.2f%%"
            println(f"$value%-30s $count%-15d $pctStr%-14s $description%-30s")
          } else {
            val pctStr = f"${pct}%.2f%%"
            println(f"$value%-30s $count%-15d $pctStr")
          }
        }
      }
    }
    
    // 4.2 Quantity Variables - Percentile distributions
    val amountColumns = Seq("trip_distance", "fare_amount", "extra", "mta_tax", 
                           "tip_amount", "tolls_amount", "improvement_surcharge")
    println("\n\n4.2 Quantity VARIABLES - Percentile Distribution")
    println("-" * 140)
    println("%-18s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s".format(
      "Column", "Min", "0.1%ile", "1st %ile", "5th %ile", "25th %ile", "median", "75th %ile", "95th %ile", "99th %ile", "99.9%ile", "Max"))
    println("-" * 140)
    
    amountColumns.foreach { colName =>
      if (df.columns.contains(colName)) {
        val stats = df.select(
          min(col(colName)).alias("min"),
          expr(s"percentile_approx($colName, 0.001)").alias("p0_1"),
          expr(s"percentile_approx($colName, 0.01)").alias("p1"),
          expr(s"percentile_approx($colName, 0.05)").alias("p5"),
          expr(s"percentile_approx($colName, 0.25)").alias("p25"),
          expr(s"percentile_approx($colName, 0.50)").alias("p50"),
          expr(s"percentile_approx($colName, 0.75)").alias("p75"),
          expr(s"percentile_approx($colName, 0.95)").alias("p95"),
          expr(s"percentile_approx($colName, 0.99)").alias("p99"),
          expr(s"percentile_approx($colName, 0.999)").alias("p99_9"),
          max(col(colName)).alias("max")
        ).collect()(0)
        
        val minVal = if (stats.get(0) == null) "NULL" else f"${stats.getAs[Double](0)}%.2f"
        val p0_1 = if (stats.get(1) == null) "NULL" else f"${stats.getAs[Double](1)}%.2f"
        val p1 = if (stats.get(2) == null) "NULL" else f"${stats.getAs[Double](2)}%.2f"
        val p5 = if (stats.get(3) == null) "NULL" else f"${stats.getAs[Double](3)}%.2f"
        val p25 = if (stats.get(4) == null) "NULL" else f"${stats.getAs[Double](4)}%.2f"
        val p50 = if (stats.get(5) == null) "NULL" else f"${stats.getAs[Double](5)}%.2f"
        val p75 = if (stats.get(6) == null) "NULL" else f"${stats.getAs[Double](6)}%.2f"
        val p95 = if (stats.get(7) == null) "NULL" else f"${stats.getAs[Double](7)}%.2f"
        val p99 = if (stats.get(8) == null) "NULL" else f"${stats.getAs[Double](8)}%.2f"
        val p99_9 = if (stats.get(9) == null) "NULL" else f"${stats.getAs[Double](9)}%.2f"
        val maxVal = if (stats.get(10) == null) "NULL" else f"${stats.getAs[Double](10)}%.2f"
        
        println(f"$colName%-18s $minVal%-9s $p0_1%-9s $p1%-9s $p5%-9s $p25%-9s $p50%-9s $p75%-9s $p95%-9s $p99%-9s $p99_9%-9s $maxVal%-9s")
      }
    }
    
    // 4.3 Flag Variable
    val flagColumn = "store_and_fwd_flag"
    if (df.columns.contains(flagColumn)) {
      println("\n\n4.3 FLAG VARIABLE -  store_and_fwd_flag (Distribution)")
      println("-" * 80)
      println("%-30s %-15s %-10s".format("Value", "Count", "Percentage"))
      println("-" * 80)
      val flagCounts = df.groupBy(flagColumn)
        .agg(count("*").alias("count"))
        .withColumn("percentage", (col("count") / totalCount) * 100)
        .orderBy(desc("count"))
        .collect()
      
      flagCounts.foreach { row =>
        val value = if (row.get(0) == null) "NULL" else row.get(0).toString
        val count = row.getAs[Long]("count")
        val pct = row.getAs[Double]("percentage")
        println(f"$value%-30s $count%-15d ${pct}%.2f%%")
      }
    }
    
    // 4.4 Duration - Calculate trip duration and show percentiles
    if (df.columns.contains("tpep_pickup_datetime") && df.columns.contains("tpep_dropoff_datetime")) {
      println("\n\n4.4 DURATION - Trip Duration Analysis (in minutes)")
      println("-" * 140)
      
      val dfWithDuration = df.withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
      )
      
      val durationStats = dfWithDuration.select(
        min("trip_duration_minutes").alias("min"),
        expr("percentile_approx(trip_duration_minutes, 0.001)").alias("p0_1"),
        expr("percentile_approx(trip_duration_minutes, 0.01)").alias("p1"),
        expr("percentile_approx(trip_duration_minutes, 0.05)").alias("p5"),
        expr("percentile_approx(trip_duration_minutes, 0.25)").alias("p25"),
        expr("percentile_approx(trip_duration_minutes, 0.50)").alias("p50"),
        expr("percentile_approx(trip_duration_minutes, 0.75)").alias("p75"),
        expr("percentile_approx(trip_duration_minutes, 0.95)").alias("p95"),
        expr("percentile_approx(trip_duration_minutes, 0.99)").alias("p99"),
        expr("percentile_approx(trip_duration_minutes, 0.999)").alias("p99_9"),
        max("trip_duration_minutes").alias("max"),
        avg("trip_duration_minutes").alias("mean")
      ).collect()(0)
      
      println("%-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s %-9s".format(
        "Min", "0.1%ile", "1st %ile", "5th %ile", "25th %ile", "median", "75th %ile", "95th %ile", "99th %ile", "99.9%ile", "Max", "Mean"))
      println("-" * 140)
      
      val minVal = if (durationStats.get(0) == null) "NULL" else f"${durationStats.getAs[Double](0)}%.2f"
      val p0_1 = if (durationStats.get(1) == null) "NULL" else f"${durationStats.getAs[Double](1)}%.2f"
      val p1 = if (durationStats.get(2) == null) "NULL" else f"${durationStats.getAs[Double](2)}%.2f"
      val p5 = if (durationStats.get(3) == null) "NULL" else f"${durationStats.getAs[Double](3)}%.2f"
      val p25 = if (durationStats.get(4) == null) "NULL" else f"${durationStats.getAs[Double](4)}%.2f"
      val p50 = if (durationStats.get(5) == null) "NULL" else f"${durationStats.getAs[Double](5)}%.2f"
      val p75 = if (durationStats.get(6) == null) "NULL" else f"${durationStats.getAs[Double](6)}%.2f"
      val p95 = if (durationStats.get(7) == null) "NULL" else f"${durationStats.getAs[Double](7)}%.2f"
      val p99 = if (durationStats.get(8) == null) "NULL" else f"${durationStats.getAs[Double](8)}%.2f"
      val p99_9 = if (durationStats.get(9) == null) "NULL" else f"${durationStats.getAs[Double](9)}%.2f"
      val maxVal = if (durationStats.get(10) == null) "NULL" else f"${durationStats.getAs[Double](10)}%.2f"
      val meanVal = if (durationStats.get(11) == null) "NULL" else f"${durationStats.getAs[Double](11)}%.2f"
      
      println(f"$minVal%-9s $p0_1%-9s $p1%-9s $p5%-9s $p25%-9s $p50%-9s $p75%-9s $p95%-9s $p99%-9s $p99_9%-9s $maxVal%-9s $meanVal%-9s")
    }
    
    // 4.5 Trip Date Distribution - Analyze pickup dates
    if (df.columns.contains("tpep_pickup_datetime")) {
      println("\n\n4.5 TRIP DATE DISTRIBUTION - Pickup Date Analysis")
      println("-" * 80)
      println("Purpose: Identify date ranges in dataset and filter invalid dates")
      println("         Expected: September 2018 (2018-09-01 to 2018-09-30)")
      println("-" * 80)
      
      // Extract date from timestamp
      val dfWithDate = df
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .withColumn("pickup_year_month", date_format(col("pickup_date"), "yyyy-MM"))
        .withColumn("pickup_year", year(col("pickup_date")))
        .withColumn("pickup_month", month(col("pickup_date")))
      
      // Count by year-month
      println("\nDistribution by Year-Month:")
      println("%-15s %-15s %-15s %-20s".format("Year-Month", "Count", "Percentage", "Status"))
      println("-" * 80)
      
      val yearMonthCounts = dfWithDate
        .filter(col("pickup_date").isNotNull)
        .groupBy("pickup_year_month")
        .agg(count("*").alias("count"))
        .withColumn("percentage", (col("count") / totalCount) * 100)
        .orderBy("pickup_year_month")
        .collect()
      
      yearMonthCounts.foreach { row =>
        val yearMonth = row.getAs[String]("pickup_year_month")
        val count = row.getAs[Long]("count")
        val pct = row.getAs[Double]("percentage")
        val status = if (yearMonth == "2018-09") "✓ Expected" else "⚠️ Unexpected"
        val pctStr = f"${pct}%.2f%%"
        println(f"$yearMonth%-15s $count%-15d $pctStr%-14s $status%-20s")
      }
      
      // Count by year for broader view
      println("\n\nDistribution by Year:")
      println("%-15s %-15s %-15s".format("Year", "Count", "Percentage"))
      println("-" * 50)
      
      val yearCounts = dfWithDate
        .filter(col("pickup_date").isNotNull)
        .groupBy("pickup_year")
        .agg(count("*").alias("count"))
        .withColumn("percentage", (col("count") / totalCount) * 100)
        .orderBy("pickup_year")
        .collect()
      
      yearCounts.foreach { row =>
        val year = row.getAs[Int]("pickup_year")
        val count = row.getAs[Long]("count")
        val pct = row.getAs[Double]("percentage")
        val pctStr = f"${pct}%.2f%%"
        println(f"$year%-15d $count%-15d $pctStr")
      }
      
      // Show date range
      println("\n\nDate Range Summary:")
      println("-" * 80)
      val dateRange = dfWithDate
        .filter(col("pickup_date").isNotNull)
        .select(
          min("pickup_date").alias("min_date"),
          max("pickup_date").alias("max_date")
        )
        .collect()(0)
      
      val minDate = dateRange.getAs[java.sql.Date]("min_date")
      val maxDate = dateRange.getAs[java.sql.Date]("max_date")
      println(f"Earliest pickup date: $minDate")
      println(f"Latest pickup date: $maxDate")
      
      // Count trips in September 2018
      val sept2018Count = dfWithDate
        .filter(
          col("pickup_year") === 2018 &&
          col("pickup_month") === 9
        )
        .count()
      
      val sept2018Pct = (sept2018Count.toDouble / totalCount) * 100
      val otherCount = totalCount - sept2018Count
      val otherPct = (otherCount.toDouble / totalCount) * 100
      
      println(f"\nTrips in September 2018: $sept2018Count%,d (${sept2018Pct}%.2f%%)")
      println(f"Trips NOT in September 2018: $otherCount%,d (${otherPct}%.2f%%)")
      
      if (otherCount > 0) {
        println("\n⚠️  WARNING: Dataset contains trips outside September 2018!")
        println("   Consider filtering these trips for accurate analysis.")
      }
    }
    
    // Step 4: Negative Values Check for Fare Components
    println("\n" + "=" * 80)
    println("STEP 4: NEGATIVE VALUES CHECK - FARE COMPONENTS")
    println("=" * 80)
    println("Checking for negative values in fare_amount related columns")
    println("-" * 80)
    
    val fareComponentColumns = Seq("fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount")
    println("%-25s %-15s %-15s %-20s".format("Column", "Negative Count", "Percentage", "Min Value"))
    println("-" * 80)
    
    fareComponentColumns.foreach { colName =>
      if (df.columns.contains(colName)) {
        val negativeCount = df.filter(col(colName) < 0).count()
        val pct = if (totalCount > 0) (negativeCount.toDouble / totalCount) * 100 else 0.0
        val minVal = df.agg(min(col(colName))).collect()(0).getAs[Double](0)
        
        val status = if (negativeCount > 0) "⚠️ ISSUE" else "✓ OK"
        val pctStr = f"${pct}%.2f%%"
        val minValStr = f"$$${minVal}%.2f"
        println(f"$colName%-25s $negativeCount%-15d $pctStr%-14s $minValStr%-19s $status")
      }
    }
    
    // Payment type breakdown for negative fare_amount records
    if (df.columns.contains("fare_amount") && df.columns.contains("payment_type")) {
      val negativeFareRecords = df.filter(col("fare_amount") < 0)
      val negativeFareCount = negativeFareRecords.count()
      
      if (negativeFareCount > 0) {
        println("\n\nPayment Type Breakdown for Records with Negative fare_amount:")
        println("-" * 80)
        
        println("%-30s %-15s %-15s %-20s".format("Payment Type", "Count", "Percentage", "Description"))
        println("-" * 80)
        
        val paymentBreakdown = negativeFareRecords
          .groupBy("payment_type")
          .agg(count("*").alias("count"))
          .withColumn("percentage", (col("count") / negativeFareCount) * 100)
          .orderBy(desc("count"))
          .collect()
        
        paymentBreakdown.foreach { row =>
          val paymentType = row.getAs[Long]("payment_type").toInt
          val count = row.getAs[Long]("count")
          val pct = row.getAs[Double]("percentage")
          val description = paymentTypeMapping.getOrElse(paymentType, s"Unknown code ($paymentType)")
          val pctStr = f"${pct}%.2f%%"
          
          println(f"$paymentType%-30d $count%-15d $pctStr%-14s $description%-20s")
        }
        
        println(s"\nTotal records with negative fare_amount: $negativeFareCount")
      }
    }
      
      spark.stop()
      
    } finally {
      // Restore original System.out before printing final message
      System.setOut(originalOut)
      teePrintStream.close()
      fileOutputStream.close()
      println(s"\nOutput also saved to: ${outputFile.getAbsolutePath}")
    }
  }
}
