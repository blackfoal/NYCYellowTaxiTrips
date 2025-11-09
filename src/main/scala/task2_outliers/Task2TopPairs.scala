package task2_toppairs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, FileOutputStream, PrintStream, OutputStream}
import org.apache.log4j.{Logger, PropertyConfigurator}
import utils.DataLoader



object Task2TopPairs {
  
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

    // Clear any existing appenders first, then load log4j.properties
    Logger.getRootLogger().removeAllAppenders()

    val log4jProps = getClass.getClassLoader.getResource("log4j.properties")
    if (log4jProps != null) {
      PropertyConfigurator.configure(log4jProps)
    }
    
    // Set up output file in the task2_outliers directory
    val sourceDir = new File("src/main/scala/task2_outliers")
    sourceDir.mkdirs()
    val outputFile = new File(sourceDir, "Task2TopPairs_output.txt")
    
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
        .appName("Task2TopPairs")
        .master("local[*]")
        .getOrCreate()
      
      import spark.implicits._

    // Accept path as argument, or use default from DataLoader
    val dataPath = if (args.length > 0) args(0) else ""
    val df = DataLoader.loadMergedData(spark, dataPath)

    println("=" * 80)
    println("TASK 2: TOP 10 LOCATION PAIRS (AFTER OUTLIER REMOVAL)")
    println("=" * 80)
    
    val totalCount = df.count()
    println(s"\nTotal records: $totalCount")
    
    // Calculate fine-grained percentiles to find the "elbow"
    println("\nAnalyzing trip_distance distribution to determine outlier threshold...")
    val percentiles = Seq(0.999, 0.9995, 0.9996, 0.9997, 0.9998, 0.99985, 0.9999)
    val percentileValues = percentiles.map { p =>
      val value = df.select(expr(s"percentile_approx(trip_distance, $p)")).collect()(0).getAs[Double](0)
      (p, value)
    }
    
    // Print percentile analysis
    println("\nPercentile Analysis:")
    println("%-12s %-15s %-15s".format("Percentile", "Value (miles)", "Jump Ratio"))
    println("-" * 45)
    percentileValues.zipWithIndex.foreach { case ((p, value), idx) =>
      val pctStr = f"${p * 100}%.3f%%"
      val valueStr = f"${value}%.2f"
      if (idx == 0) {
        println(f"$pctStr%-12s $valueStr%-15s ${"-"}%-15s")
      } else {
        val prev = percentileValues(idx - 1)._2
        val jump = value / prev
        val jumpStr = if (jump > 2.0) f"${jump}%.2fx ⚠️" else f"${jump}%.2fx"
        println(f"$pctStr%-12s $valueStr%-15s $jumpStr%-15s")
      }
    }
    
    // Find where the jump becomes dramatic (e.g., > 2x previous value)
    var breakpoint = percentileValues.last._2
    var breakpointPercentile = percentiles.last
    for (i <- 1 until percentileValues.length) {
      val prev = percentileValues(i - 1)._2
      val curr = percentileValues(i)._2
      val jump = curr / prev
      if (jump > 2.0) {  // If jump is more than 2x, use previous as breakpoint
        breakpoint = prev
        breakpointPercentile = percentiles(i - 1)
        // Add a buffer (2x) to be slightly above the "normal" range
        breakpoint = prev * 2
      }
    }
    
    println(f"\nOutlier Threshold: ${breakpoint}%.2f miles (based on ${breakpointPercentile * 100}%.3f%% percentile with jump detection)")
    
    // Filter outliers
    val dfFiltered = df.filter(col("trip_distance") <= breakpoint)
    val filteredCount = dfFiltered.count()
    val removedCount = totalCount - filteredCount
    val removedPct = (removedCount.toDouble / totalCount) * 100
    
    println(f"\nOutlier Removal:")
    println(f"  Records removed: $removedCount (${removedPct}%.2f%%)")
    println(f"  Records remaining: $filteredCount")
    
    // Calculate top 10 location pairs by total_amount
    println("\n" + "=" * 80)
    println("TOP 10 PULocationId, DOLocationId PAIRS BY TOTAL_AMOUNT")
    println("=" * 80)
    
    val topPairs = dfFiltered
      .filter(col("PULocationID").isNotNull && col("DOLocationID").isNotNull && col("total_amount").isNotNull)
      .groupBy("PULocationID", "DOLocationID")
      .agg(
        sum("total_amount").alias("total_amount_sum"),
        count("*").alias("trip_count"),
        avg("trip_distance").alias("avg_distance"),
        avg("total_amount").alias("avg_amount")
      )
      .orderBy(desc("total_amount_sum"))
      .limit(10)
      .collect()
    
    println("\n%-12s %-12s %-18s %-12s %-15s %-15s".format(
      "PULocationID", "DOLocationID", "Total Amount", "Trip Count", "Avg Distance", "Avg Amount"))
    println("-" * 100)
    
    topPairs.zipWithIndex.foreach { case (row, idx) =>
      val puLoc = row.getAs[Long]("PULocationID")
      val doLoc = row.getAs[Long]("DOLocationID")
      val totalAmt = row.getAs[Double]("total_amount_sum")
      val tripCount = row.getAs[Long]("trip_count")
      val avgDist = row.getAs[Double]("avg_distance")
      val avgAmt = row.getAs[Double]("avg_amount")
      
      val totalAmtStr = f"$$${totalAmt}%.2f"
      val tripCountStr = f"${tripCount}%,d"
      val avgDistStr = f"${avgDist}%.2f"
      val avgAmtStr = f"$$${avgAmt}%.2f"
      
      println(f"${idx + 1}%-2d ${puLoc}%-12d ${doLoc}%-12d $totalAmtStr%-18s $tripCountStr%-12s $avgDistStr%-15s $avgAmtStr%-15s")
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