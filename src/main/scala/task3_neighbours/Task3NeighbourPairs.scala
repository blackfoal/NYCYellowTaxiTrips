package task3_neighbourpairs

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.io.{File, FileOutputStream, PrintStream, OutputStream}
import org.apache.log4j.{Logger, PropertyConfigurator}
import utils.DataLoader



object Task3NeighbourPairs {
  
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
    
    // Set up output file in the task3_neighbours directory
    val sourceDir = new File("src/main/scala/task3_neighbours")
    sourceDir.mkdirs()
    val outputFile = new File(sourceDir, "Task3NeighbourPairs_output.txt")
    
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
        .appName("Task3NeighbourPairs")
        .master("local[*]")
        .getOrCreate()
      
      import spark.implicits._

    // Accept path as argument, or use default from DataLoader
    val dataPath = if (args.length > 0) args(0) else ""
    val df = DataLoader.loadMergedData(spark, dataPath)

    println("=" * 80)
    println("TASK 3: TOP 10 LOCATION PAIRS (INCLUDING NEIGHBOURS)")
    println("=" * 80)
    
    println("\n" + "-" * 80)
    println("OUTLIER REMOVAL")
    println("-" * 80)

    val totalCount = df.count()
    println(s"\nTotal records: $totalCount")
    
    // Calculate fine-grained percentiles to find the "elbow"
    println("\nUsing the result from Task2: 73,86 miles as outlier threshold...")
    
    // Filter outliers
    val dfFiltered = df.filter(col("trip_distance") <= 73.86)
    val filteredCount = dfFiltered.count()
    val removedCount = totalCount - filteredCount
    val removedPct = (removedCount.toDouble / totalCount) * 100
    
    println(f"\nOutlier Removal:")
    println(f"  Records removed: $removedCount (${removedPct}%.2f%%)")
    println(f"  Records remaining: $filteredCount")
    println("\n" + "-" * 80)

    println("\n" + "=" * 80)
    println("TOP 10 LOCATION PAIRS WITH NEIGHBOR INCLUSION")
    println("=" * 80)
    println("\nThis task calculates top 10 location pairs by total_amount, where each pair")
    println("includes contributions from its 3x3 neighborhood (neighboring locations).")
    println("\nExample: Pair (5,5) includes trips from:")
    println("  (4,4), (4,5), (4,6), (5,4), (5,5), (5,6), (6,4), (6,5), (6,6)")
    println("\nNote: Outlier filtering has been applied (trip_distance <= 73.86 miles)")
    println("      Only valid trips with non-null locations and amounts are included.")
    println("\nOptimization: Aggregate first, then expand (avoids expanding millions of trips)")

    // ============================================================================
    // STEP 1: Aggregate trips by actual (PU, DO) pairs
    // ============================================================================
    println("\n" + "-" * 80)
    println("STEP 1: AGGREGATE TRIPS BY ACTUAL LOCATION PAIRS")
    println("-" * 80)
    println("Purpose: Reduce data volume by grouping trips with same (PULocationID, DOLocationID)")
    println("         before expanding to neighbors. This is much more efficient.")
    
    val aggregatedPairs = dfFiltered
      .filter(col("PULocationID").isNotNull && col("DOLocationID").isNotNull && col("total_amount").isNotNull)
      .groupBy("PULocationID", "DOLocationID")
      .agg(
        sum("total_amount").alias("total_amount_sum"),
        count("*").alias("trip_count")
      )
    
    val uniquePairsCount = aggregatedPairs.count()
    println(f"\nResult: $uniquePairsCount%,d unique (PULocationID, DOLocationID) pairs found")
    println("        (down from " + filteredCount + " individual trips)")
    
    // Show sample of aggregated pairs
    println("\nSample of aggregated pairs (first 20):")
    println("%-15s %-15s %-20s %-15s".format("PULocationID", "DOLocationID", "Total Amount", "Trip Count"))
    println("-" * 70)
    aggregatedPairs
      .orderBy(desc("total_amount_sum"))
      .limit(10)
      .collect()
      .foreach { row =>
        val puLoc = row.getAs[Long]("PULocationID")
        val doLoc = row.getAs[Long]("DOLocationID")
        val totalAmt = row.getAs[Double]("total_amount_sum")
        val tripCount = row.getAs[Long]("trip_count")
        val totalAmtStr = f"$$${totalAmt}%,.2f"
        val tripCountStr = f"${tripCount}%,d"
        println(f"$puLoc%-15d $doLoc%-15d $totalAmtStr%-20s $tripCountStr%-15s")
      }
    
    // ============================================================================
    // STEP 2: Expand each aggregated pair to 9 canonical pairs (3x3 neighbor grid)
    // ============================================================================
    println("\n" + "-" * 80)
    println("STEP 2: EXPAND TO CANONICAL PAIRS (3x3 NEIGHBOR GRID)")
    println("-" * 80)
    println("Purpose: Each pair (x, y) generates 9 canonical pairs representing its neighborhood:")
    println("         (x-1,y-1), (x-1,y), (x-1,y+1), (x,y-1), (x,y), (x,y+1), (x+1,y-1), (x+1,y), (x+1,y+1)")
    println("         Each canonical pair receives the same total_amount from the original pair.")
    println("\nProcessing...")
    
    val tripsWithCanonicalPairs = aggregatedPairs
      .select(
        col("PULocationID").cast(LongType).alias("puLoc"),
        col("DOLocationID").cast(LongType).alias("doLoc"),
        col("total_amount_sum").cast(DoubleType),
        col("trip_count").cast(LongType)
      )
      .flatMap { row =>
        val puLoc = row.getAs[Long]("puLoc")
        val doLoc = row.getAs[Long]("doLoc")
        val totalAmt = row.getAs[Double]("total_amount_sum")
        val tripCount = row.getAs[Long]("trip_count")
        
        // Generate all 9 canonical pairs (3x3 grid)
        // Each pair (x,y) contributes its total_amount to all 9 neighbors
        for {
          puOffset <- -1 to 1
          doOffset <- -1 to 1
          canonicalPU = puLoc + puOffset
          canonicalDO = doLoc + doOffset
          if canonicalPU > 0 && canonicalDO > 0  // Filter invalid locations (≤ 0)
        } yield (canonicalPU, canonicalDO, totalAmt, tripCount)
      }
      .toDF("canonical_PU", "canonical_DO", "total_amount", "trip_count")
    
    val expandedRowsCount = tripsWithCanonicalPairs.count()
    val expansionRatio = expandedRowsCount.toDouble / uniquePairsCount
    println(f"\nResult: $expandedRowsCount%,d rows after expansion (from $uniquePairsCount%,d unique pairs)")
    println(f"        Average expansion: ${expansionRatio}%.2fx (slightly less than 9x due to edge filtering)")

    // ============================================================================
    // STEP 3: Aggregate by canonical pairs and get top 10
    // ============================================================================
    println("\n" + "-" * 80)
    println("STEP 3: AGGREGATE BY CANONICAL PAIRS AND FIND TOP 10")
    println("-" * 80)
    println("Purpose: Sum total_amount for each canonical pair across all contributing neighbors.")
    println("         Each canonical pair now includes contributions from its 3x3 neighborhood.")
    println("\nProcessing...")
    
    val topPairs = tripsWithCanonicalPairs
      .groupBy("canonical_PU", "canonical_DO")
      .agg(
        sum("total_amount").alias("total_amount_sum"),
        sum("trip_count").alias("total_trip_count")
      )
      .orderBy(desc("total_amount_sum"))
      .limit(10)
    
    val topPairsCount = topPairs.count()
    println(f"\nResult: Top $topPairsCount pairs identified")
    
    // ============================================================================
    // FINAL RESULTS
    // ============================================================================
    println("\n" + "=" * 80)
    println("TOP 10 CANONICAL LOCATION PAIRS BY TOTAL_AMOUNT (INCLUDING NEIGHBORS)")
    println("=" * 80)
    println("\nEach pair includes contributions from its 3x3 neighborhood.")
    println("The total_amount represents the sum from all neighboring location pairs.\n")
    
    println("%-5s %-15s %-15s %-20s %-20s".format(
      "Rank", "PULocationID", "DOLocationID", "Total Amount", "Total Trip Count"))
    println("-" * 80)
    
    topPairs.collect().zipWithIndex.foreach { case (row, idx) =>
      val puLoc = row.getAs[Long]("canonical_PU")
      val doLoc = row.getAs[Long]("canonical_DO")
      val totalAmt = row.getAs[Double]("total_amount_sum")
      val tripCount = row.getAs[Long]("total_trip_count")
      
      val totalAmtStr = f"$$${totalAmt}%,.2f"
      val tripCountStr = f"${tripCount}%,d"
      val rank = idx + 1
      
      println(f"$rank%-5d ${puLoc}%-15d ${doLoc}%-15d $totalAmtStr%-20s $tripCountStr%-20s")
    }
    
      println("\n" + "=" * 80)
      println("ANALYSIS COMPLETE")
      println("=" * 80)
      println("\nNote: These results differ from Task 2 because each canonical pair")
      println("      includes revenue from its neighboring location pairs, not just")
      println("      the exact (PU, DO) combination.")

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