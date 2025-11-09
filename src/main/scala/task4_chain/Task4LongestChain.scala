package task4_chain

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import java.io.{File, FileOutputStream, PrintStream, OutputStream}
import org.apache.log4j.{Logger, PropertyConfigurator}
import utils.DataLoader


object Task4LongestChain {
  
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

    // Set up output file in the task4_chain directory
    // Get the directory where this source file is located
    val sourceDir = new File("src/main/scala/task4_chain")
    sourceDir.mkdirs()
    val outputFile = new File(sourceDir, "Task4LongestChain_output.txt")
    
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
        .appName("Task4LongestChain")
        .master("local[*]")
        .getOrCreate()
      
      import spark.implicits._

      // Accept path as argument, or use default from DataLoader
      val dataPath = if (args.length > 0) args(0) else ""
      val df = DataLoader.loadMergedData(spark, dataPath)

      println("=" * 80)
      println("TASK 4: LONGEST SINGLE-DRIVER CHAIN OF TRIPS")
      println("=" * 80)
      println("\nAssumption: Same driver if:")
      println("  - Next trip's pickup location = Previous trip's dropoff location")
      println("  - Next trip's pickup time < Previous trip's dropoff time + 5 minutes")
      println("=" * 80)

      // ============================================================================
      // STEP 1: LOAD AND FILTER DATA
      // ============================================================================
      println("\n" + "-" * 80)
      println("STEP 1: LOADING AND FILTERING DATA")
      println("-" * 80)
      
      val step1Start = System.nanoTime()
      
      val totalCount = df.count()
      println(s"\nTotal records: $totalCount")
      
      // Filter for valid trips: must have locations, timestamps, and reasonable trip_distance
      // Using the same outlier threshold from Task 2: 73.86 miles
      val dfFiltered = df
        .filter(
          col("PULocationID").isNotNull &&
          col("DOLocationID").isNotNull &&
          col("tpep_pickup_datetime").isNotNull &&
          col("tpep_dropoff_datetime").isNotNull &&
          col("trip_distance") <= 73.86
        )
      
      val filteredCount = dfFiltered.count()
      val removedCount = totalCount - filteredCount
      val removedPct = (removedCount.toDouble / totalCount) * 100
      
      println(f"\nFiltering results:")
      println(f"  Records removed: $removedCount (${removedPct}%.2f%%)")
      println(f"  Valid records: $filteredCount")
      
      val step1DurationSeconds = (System.nanoTime() - step1Start).toDouble / 1e9
      println(f"\nStep 1 took ${step1DurationSeconds}%.2f seconds")

      // ============================================================================
      // STEP 2: ADD COMPUTED COLUMNS
      // ============================================================================
      println("\n" + "-" * 80)
      println("STEP 2: ADDING COMPUTED COLUMNS")
      println("-" * 80)
      println("Purpose: Convert timestamps to Unix seconds for efficient comparisons")
      println("         Add trip_id for tracking individual trips")
      
      val step2Start = System.nanoTime()
      
      val dfWithComputed = dfFiltered
        .withColumn(
          "pickup_unix",
          unix_timestamp(col("tpep_pickup_datetime"))
        )
        .withColumn(
          "dropoff_unix",
          unix_timestamp(col("tpep_dropoff_datetime"))
        )
        .filter(
          col("pickup_unix").isNotNull &&
          col("dropoff_unix").isNotNull &&
          col("dropoff_unix") >= col("pickup_unix") &&  // Sanity check: dropoff after pickup
          // Filter for September 2018 only
          // September 2018: 2018-09-01 00:00:00 to 2018-09-30 23:59:59
          // Unix timestamps: 1535760000 to 1538351999
          col("pickup_unix") >= 1535760000 &&  // After 2018-09-01 00:00:00
          col("pickup_unix") <= 1538351999     // Before 2018-10-01 00:00:00
        )
      
      val validTimeCount = dfWithComputed.count()
      val dateFilteredCount = dfFiltered
        .filter(
          col("tpep_pickup_datetime").isNotNull &&
          col("tpep_dropoff_datetime").isNotNull
        )
        .count()
      val dateFilteredRemoved = dateFilteredCount - validTimeCount
      val dateFilteredPct = if (dateFilteredCount > 0) (dateFilteredRemoved.toDouble / dateFilteredCount) * 100 else 0.0
      
      println(f"\nTrips with valid timestamps: $validTimeCount")
      if (dateFilteredRemoved > 0) {
        println(f"Trips filtered out (outside Sept 2018 or invalid): $dateFilteredRemoved (${dateFilteredPct}%.2f%%)")
      }
      
      val step2DurationSeconds = (System.nanoTime() - step2Start).toDouble / 1e9
      println(f"\nStep 2 took ${step2DurationSeconds}%.2f seconds")

    // ============================================================================
    // STEP 3: SORT TRIPS BY PICKUP TIME AND ADD TRIP ID
    // ============================================================================
    println("\n" + "-" * 80)
    println("STEP 3: SORTING TRIPS BY PICKUP TIME")
    println("-" * 80)
    println("Purpose: Sort all trips chronologically to enable chain building")
    println("         Add trip_id for tracking and joining")
    
    val step3Start = System.nanoTime()
    
    val dfSorted = dfWithComputed
      .orderBy("pickup_unix")
      .withColumn("trip_id", monotonically_increasing_id())
      .cache()  // Cache since we'll use it multiple times
    
    println(f"\nTrips sorted by pickup time")
    println("Sample of first 5 trips:")
    println("%-15s %-15s %-15s %-20s %-20s".format(
      "Trip ID", "PULocationID", "DOLocationID", "Pickup Time", "Dropoff Time"))
    println("-" * 90)
    
    dfSorted
      .select("trip_id", "PULocationID", "DOLocationID", "tpep_pickup_datetime", "tpep_dropoff_datetime")
      .limit(5)
      .collect()
      .foreach { row =>
        val tripId = row.getAs[Long]("trip_id")
        val puLoc = row.getAs[Long]("PULocationID")
        val doLoc = row.getAs[Long]("DOLocationID")
        val pickupTime = Option(row.getAs[Any]("tpep_pickup_datetime")).map(_.toString).getOrElse("NULL")
        val dropoffTime = Option(row.getAs[Any]("tpep_dropoff_datetime")).map(_.toString).getOrElse("NULL")
        println(f"$tripId%-15d $puLoc%-15d $doLoc%-15d $pickupTime%-20s $dropoffTime%-20s")
      }
    
    val step3DurationSeconds = (System.nanoTime() - step3Start).toDouble / 1e9
    println(f"\nStep 3 took ${step3DurationSeconds}%.2f seconds")

    // ============================================================================
    // STEP 4: COMPUTE CHAIN LENGTHS (SINGLE-PASS ALGORITHM)
    // ============================================================================
    println("\n" + "-" * 80)
    println("STEP 4: COMPUTING CHAIN LENGTHS (SINGLE-PASS)")
    println("-" * 80)
    println("Purpose: Process all trips chronologically in a single pass")
    println("         Use sliding window buffer to find best predecessor")
    println("\nAlgorithm:")
    println("  1. Sort all trips globally by pickup_unix")
    println("  2. Process trips sequentially in chronological order")
    println("  3. Maintain location-indexed buffer of recent trips (5-minute window)")
    println("  4. For each trip, find best predecessor from buffer")
    println("  5. Compute chain_length = best_prev_chain_length + 1")
    println("\nNote: Isolated trips (no predecessors) will have chain_length = 1")
    println("      They don't affect the longest chain computation")
    
    val step4Start = System.nanoTime()
    
    // Prepare trips with all needed columns
    // Use dfSorted directly - no need to filter isolated trips
    val tripsPrepared = dfSorted
      .select(
        col("trip_id"),
        col("PULocationID"),
        col("DOLocationID"),
        col("pickup_unix"),
        col("dropoff_unix")
      )
    
    // CRITICAL: Use single partition to process ALL trips globally
    // This ensures we find chains that span across what would be partition boundaries
    // Sort globally by pickup_unix to process chronologically
    val tripsGlobal = tripsPrepared
      .coalesce(1)  // Single partition - processes all data globally
      .orderBy("pickup_unix")  // Global sort ensures chronological processing
    
    val totalTripsCount = tripsPrepared.count()
    println(f"\nStarting single-pass chain computation on $totalTripsCount trips")
    println("Processing trips chronologically with sliding window buffer...")
    
    // Use mapPartitions to process the single partition sequentially
    // Maintains a sliding window buffer organized by dropoff location
    val chainLengthsRDD = tripsGlobal.rdd.mapPartitions { partition =>
      // Buffer organized by dropoff location for O(1) predecessor lookup
      // Map[DOLocationID -> ArrayBuffer of (trip_id, dropoff_unix, chain_length, prev_trip_id)]
      val locationBuffers = scala.collection.mutable.HashMap[Long, 
        scala.collection.mutable.ArrayBuffer[(Long, Long, Int, Option[Long])]]()
      
      partition.map { row =>
        val tripId = row.getAs[Long]("trip_id")
        val puLoc = row.getAs[Long]("PULocationID")
        val doLoc = row.getAs[Long]("DOLocationID")
        val pickupUnix = row.getAs[Long]("pickup_unix")
        val dropoffUnix = row.getAs[Long]("dropoff_unix")
        
        // Find best predecessor: look in buffer for trips that dropped off at puLoc
        // A trip can be a predecessor if:
        // 1. Its dropoff location == current pickup location (prev.DOLocationID == curr.PULocationID)
        // 2. Its dropoff time < current pickup time
        // 3. Time gap <= 300 seconds (5 minutes)
        val windowStart = pickupUnix - 300
        var bestChainLength = 0
        var bestPrevTripId: Option[Long] = None
        
        // Check buffer for trips that dropped off at the current pickup location
        locationBuffers.get(puLoc).foreach { buffer =>
          buffer.foreach { case (prevTripId, prevDropoffUnix, prevChainLength, _) =>
            // Check time constraints
            if (prevDropoffUnix < pickupUnix && (pickupUnix - prevDropoffUnix) <= 300) {
              if (prevChainLength > bestChainLength) {
                bestChainLength = prevChainLength
                bestPrevTripId = Some(prevTripId)
              }
            }
          }
        }
        
        // Compute chain length for current trip
        val chainLength = bestChainLength + 1
        
        // Add current trip to buffer for its dropoff location
        // This allows future trips picking up at doLoc to find this trip as a predecessor
        val buffer = locationBuffers.getOrElseUpdate(doLoc, 
          scala.collection.mutable.ArrayBuffer[(Long, Long, Int, Option[Long])]())
        buffer.append((tripId, dropoffUnix, chainLength, bestPrevTripId))
        
        // Clean up old entries from all buffers to keep memory bounded
        // Remove trips whose dropoff is more than 5 minutes before current pickup
        // This ensures buffer only contains trips within the 5-minute window
        locationBuffers.foreach { case (_, buf) =>
          buf.filterInPlace { case (_, prevDropoffUnix, _, _) =>
            prevDropoffUnix >= windowStart
          }
        }
        
        // Return result row
        Row(tripId, chainLength, bestPrevTripId.orNull)
      }
    }
    
    // Convert RDD back to DataFrame
    val chainLengthsSchema = StructType(Array(
      StructField("trip_id", LongType, nullable = false),
      StructField("chain_length", IntegerType, nullable = false),
      StructField("prev_trip_id", LongType, nullable = true)
    ))
    
    val chainLengths = spark.createDataFrame(chainLengthsRDD, chainLengthsSchema)
      .cache()
    
    val maxChainLength = chainLengths.agg(max("chain_length")).collect()(0).getAs[Int](0)
    println(f"\nMaximum chain length found: $maxChainLength")
    
    // Show some statistics
    val chainLengthStats = chainLengths
      .agg(
        count("*").alias("total_trips"),
        max("chain_length").alias("max_chain"),
        avg("chain_length").alias("avg_chain")
      )
      .collect()(0)
    
    val processedTrips = chainLengthStats.getAs[Long]("total_trips")
    val avgChain = chainLengthStats.getAs[Double]("avg_chain")
    
    println(f"Total trips processed: $processedTrips")
    println(f"Average chain length: ${avgChain}%.2f")
    
    val step4DurationSeconds = (System.nanoTime() - step4Start).toDouble / 1e9
    println(f"\nStep 4 took ${step4DurationSeconds}%.2f seconds")

    // ============================================================================
    // STEP 5: FIND MAXIMUM CHAIN AND RECONSTRUCT IT
    // ============================================================================
    println("\n" + "-" * 80)
    println("STEP 5: RECONSTRUCTING LONGEST CHAIN")
    println("-" * 80)
    
    val step5Start = System.nanoTime()
    
    // Find the trip(s) with maximum chain length
    val maxChainTrips = chainLengths
      .filter(col("chain_length") === maxChainLength)
      .orderBy("trip_id")
      .limit(1)  // If multiple, pick the first one
    
    val maxChainTrip = maxChainTrips.collect()(0)
    val endTripId = maxChainTrip.getAs[Long]("trip_id")
    
    println(f"\nLongest chain ends at trip ID: $endTripId")
    println(f"Chain length: $maxChainLength trips")
    
    // Reconstruct the chain by following prev_trip_id pointers backwards
    var currentTripId: Option[Long] = Some(endTripId)
    var chainTripIds = scala.collection.mutable.ListBuffer[Long]()
    
    while (currentTripId.isDefined) {
      val tripId = currentTripId.get
      chainTripIds.prepend(tripId)
      
      val currentTrip = chainLengths
        .filter(col("trip_id") === tripId)
        .collect()(0)
      
      val prevId = currentTrip.getAs[Any]("prev_trip_id")
      currentTripId = if (prevId == null) {
        None
      } else {
        Some(prevId.asInstanceOf[Long])
      }
    }
    
    println(f"\nChain contains ${chainTripIds.length} trips")
    
    // Join with original trip data to get full details
    val chainTripIdsList = chainTripIds.toList
    val chainTrips = dfSorted
      .filter(col("trip_id").isin(chainTripIdsList: _*))
      .orderBy("pickup_unix")
    
    val step5DurationSeconds = (System.nanoTime() - step5Start).toDouble / 1e9
    println(f"\nStep 5 took ${step5DurationSeconds}%.2f seconds")
    
    // ============================================================================
    // FINAL RESULTS
    // ============================================================================
    println("\n" + "=" * 80)
    println("LONGEST SINGLE-DRIVER CHAIN OF TRIPS")
    println("=" * 80)
    println(f"\nChain Length: $maxChainLength trips")
    println(f"Total trips in dataset: $validTimeCount")
    println(f"Chain represents: ${(maxChainLength.toDouble / validTimeCount * 100)}%.4f%% of all trips")
    
    // Create separate file for chain details (too large for stdout)
    val chainDetailsFile = new File(sourceDir, "Task4LongestChain_details.txt")
    val chainDetailsWriter = new PrintStream(new FileOutputStream(chainDetailsFile, false), true, "UTF-8")
    
    try {
      chainDetailsWriter.println("=" * 80)
      chainDetailsWriter.println("LONGEST SINGLE-DRIVER CHAIN OF TRIPS - DETAILED LISTING")
      chainDetailsWriter.println("=" * 80)
      chainDetailsWriter.println(f"\nChain Length: $maxChainLength trips")
      chainDetailsWriter.println(f"Total trips in dataset: $validTimeCount")
      chainDetailsWriter.println(f"Chain represents: ${(maxChainLength.toDouble / validTimeCount * 100)}%.4f%% of all trips")
      
      chainDetailsWriter.println("\n" + "-" * 80)
      chainDetailsWriter.println("CHAIN DETAILS (in chronological order)")
      chainDetailsWriter.println("-" * 80)
      chainDetailsWriter.println("%-5s %-15s %-15s %-25s %-25s %-15s".format(
        "Seq", "PULocationID", "DOLocationID", "Pickup Time", "Dropoff Time", "Time Gap"))
      chainDetailsWriter.println("-" * 110)
      
      var prevDropoff: Option[Long] = None
      chainTrips.collect().zipWithIndex.foreach { case (row, idx) =>
        val seq = idx + 1
        val puLoc = row.getAs[Long]("PULocationID")
        val doLoc = row.getAs[Long]("DOLocationID")
        val pickupTime = Option(row.getAs[Any]("tpep_pickup_datetime")).map(_.toString).getOrElse("NULL")
        val dropoffTime = Option(row.getAs[Any]("tpep_dropoff_datetime")).map(_.toString).getOrElse("NULL")
        val pickupUnix = row.getAs[Long]("pickup_unix")
        
        val timeGap = prevDropoff match {
          case Some(prev) => 
            val gap = pickupUnix - prev
            if (gap <= 300) f"${gap}s" else "N/A"
          case None => "START"
        }
        
        chainDetailsWriter.println(f"$seq%-5d $puLoc%-15d $doLoc%-15d $pickupTime%-25s $dropoffTime%-25s $timeGap%-15s")
        
        prevDropoff = Some(row.getAs[Long]("dropoff_unix"))
      }
      
      chainDetailsWriter.println("\n" + "=" * 80)
      chainDetailsWriter.println("END OF CHAIN DETAILS")
      chainDetailsWriter.println("=" * 80)
      
      println(f"\nChain details saved to: ${chainDetailsFile.getAbsolutePath}")
    } finally {
      chainDetailsWriter.close()
    }
    
    println("\n" + "=" * 80)
    println("ANALYSIS COMPLETE")
    println("=" * 80)

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

