package utils

import org.apache.spark.sql.{SparkSession, DataFrame}
import java.io.File

/**
 * Utility object for loading and merging Parquet files.
 * 
 * Usage:
 *   val df = DataLoader.loadMergedData(spark, "/path/to/parquet/directory")
 */
object DataLoader {
  
  /**
   * Gets the default data path relative to the project directory.
   * Looks for yellow_tripdata in the parent directory of the project.
   * 
   * @return Absolute path to the default yellow_tripdata directory
   */
  def getDefaultDataPath(): String = {
    val projectDir = new File(".").getCanonicalFile
    val parentDir = projectDir.getParentFile
    if (parentDir != null) {
      new File(parentDir, "yellow_tripdata").getAbsolutePath
    } else {
      // Fallback if parent directory can't be determined
      new File(projectDir, "yellow_tripdata").getAbsolutePath
    }
  }
  
  /**
   * Loads all Parquet files from a directory and merges them.
   * 
   * @param spark SparkSession instance
   * @param dataPath Path to directory containing Parquet files. If empty or null, uses default path.
   * @param verbose If true, prints progress information (default: false)
   * @return DataFrame with merged data
   */
  def loadMergedData(spark: SparkSession, dataPath: String = "", verbose: Boolean = false): DataFrame = {
    // Use default path if none provided
    val actualDataPath = if (dataPath == null || dataPath.isEmpty) {
      getDefaultDataPath()
    } else {
      dataPath
    }
    
    val parquetDir = new File(actualDataPath)
    
    if (!parquetDir.isDirectory) {
      throw new IllegalArgumentException(s"Path is not a directory: $actualDataPath")
    }
    
    val parquetFiles = parquetDir.listFiles()
      .filter(_.getName.endsWith(".parquet"))
      .sortBy(_.getName)
    
    if (parquetFiles.isEmpty) {
      throw new IllegalArgumentException(s"No Parquet files found in: $actualDataPath")
    }
    
    if (verbose) {
      println(s"\nLoading ${parquetFiles.length} Parquet files from: $actualDataPath")
    }
    
    // Read files and union them
    val dataFrames = parquetFiles.map { file =>
      if (verbose) {
        println(s"  Processing: ${file.getName}")
      }
      spark.read.parquet(file.getAbsolutePath)
    }
    
    if (verbose) {
      println("Unioning all files...")
    }
    
    // Union all dataframes
    val mergedDf = dataFrames.reduce(_.union(_))
    
    if (verbose) {
      println(s"\nData merged successfully. Total columns: ${mergedDf.schema.fields.length}")
    }
    
    mergedDf
  }
}

