# NYCYellowTaxiTrips - Taxi Trip Data Analysis

Apache Spark application written in Scala for analyzing NYC taxi trip data.

## Quick Start

```bash
cd NYCYellowTaxiTrips
sbt compile
sbt "runMain task1_exploration.Task1Exploration"
```

Default data path: `../yellow_tripdata/`. To use a custom path, provide it as an argument:
```bash
sbt "runMain task1_exploration.Task1Exploration /path/to/data"
```

## Requirements

- **Java 11**
- **Scala 2.13.12**
- **sbt 1.11.7+**
- **Apache Spark 3.5.0** (managed by sbt)

Dependencies are automatically resolved by sbt from `build.sbt`.

**Note:** The `build.sbt` file sets `javaHome` to a specific Java 11 path (configured for macOS with Homebrew). If you're using a different system or Java installation, you may need to update the `javaHome` path in `build.sbt` to point to your Java 11 installation, or remove that line if you have Java 11 set as your default `JAVA_HOME`.

## Project Structure

The project has the following directory structure:

```
SomeDirectory/
  ├── NYCYellowTaxiTrips/   # This project directory
  │   ├── src/
  │   │   └── main/
  │   │       └── scala/
  │   │           ├── task1_exploration/
  │   │           ├── task2_outliers/
  │   │           ├── task3_neighbours/
  │   │           ├── task4_chain/
  │   │           └── utils/
  │   ├── build.sbt
  │   └── README.md
  └── yellow_tripdata/      # Parquet data files (sibling to NYCYellowTaxiTrips)
      └── yellow_tripdata_2018-09.parquet
```

### DataLoader Utility

The `DataLoader` utility is a shared component used across all tasks to avoid code repetition. It loads and merges Parquet files from a directory.

**Important:** For simplicity, the DataLoader does not handle schema merging across different Parquet files. Please ensure only `yellow_tripdata_2018-09.parquet` is present in the data directory, as other files may have schema mismatches.

**Note:** All tasks save their output to text files in their respective task directories. For convenience, you can view these output files to see results without rerunning the tasks.

## Task 1: Data Exploration

Calculate the metrics/dimensions which, in your opinion, are the most suitable in order to understand and get familiar with the dataset.

**Approach:**
- Categorized columns into different types (categorical, numerical, flags, etc.) and analyzed each category with specific metrics
- Checked for outlier values (e.g., negative fare amounts) and investigated their payment type breakdown
- Found that negative fare amounts are primarily "no charge" or "dispute" payments, indicating business logic rather than data quality issues
- Identified trips outside September 2018 and filtered them out for subsequent analysis

```bash
sbt "runMain task1_exploration.Task1Exploration"
# Or with custom path:
sbt "runMain task1_exploration.Task1Exploration /path/to/your/data"
```

## Task 2: Top 10 Location Pairs (After Outlier Removal)

Remove trip_distance outliers from the dataset provided and calculate the top 10 PULocationId, DOLocationId pairs for total_amount. For outliers detection, you can use any algorithm that you think is suitable.

**Approach:**
- From Task 1 analysis, identified that most outliers reside after the 99.9th percentile
- Calculated fine-grained percentiles (99.9%, 99.95%, 99.96%, etc.) to find the "elbow" in the distribution
- Defined the threshold as the point where the jump ratio between consecutive percentiles exceeds 2x (indicating a dramatic increase)
- Added a 2x safety factor above the identified breakpoint to filter outliers
- Applied simple groupby aggregation on (PULocationID, DOLocationID) ordered by total_amount_sum to display top 10 pairs

```bash
sbt "runMain task2_outliers.Task2TopPairs"
# Or with custom path:
sbt "runMain task2_outliers.Task2TopPairs /path/to/your/data"
```

## Task 3: Top 10 Location Pairs (Including Neighbors)

Calculate the same as in the previous step, but this time let's assume the pair includes values from neighbouring numbers, i.e. pair (5,5) includes (4,4), (4,5), (5,4), (5,5), (4,6), (6,4), (5,6), (6,5), (6,6)

**Approach:**
- Uses the outlier threshold from Task 2 (73.86 miles) without recalculating
- **Optimization: Aggregate first, then expand** - Groups trips by actual (PULocationID, DOLocationID) pairs before neighbor expansion to reduce data volume
- Uses `flatMap` to expand each aggregated pair into 9 canonical pairs representing its 3x3 neighborhood (offsets: -1, 0, +1 for both PU and DO)
- Filters invalid locations (≤ 0) during expansion
- Aggregates by canonical pairs, summing total_amount and trip_count from all contributing neighbors
- Orders by total_amount_sum to get top 10 pairs

**Key advantage:** By working on pre-aggregated data, we expand ~thousands of unique pairs instead of millions of individual trips, making the expansion step much more efficient.

```bash
sbt "runMain task3_neighbours.Task3NeighbourPairs"
# Or with custom path:
sbt "runMain task3_neighbours.Task3NeighbourPairs /path/to/your/data"
```

## Task 4: Longest Driver Chain

Let's assume that in the data, the same driver starts every trip in the same locationId as the previous one, and less than 5 minutes after drop-off; if these conditions are not met, it means the driver is different. Under this assumption, calculate the longest possible single-driver chain of trips from the provided dataset.

**Approach:**

This was the most challenging task. At first, I tried an iterative join-based approach: find all valid predecessor relationships with a self-join (matching dropoff location to pickup location within a 5-minute window), then iteratively update chain lengths using dynamic programming until convergence. The idea was sound, but it completely failed in practice.

**Why the iterative approach failed:**
- With 8M+ trips, the self-join created massive intermediate DataFrames
- Each iteration added more transformations to Spark's query plan, creating extremely long lineage
- Even with checkpoints every few iterations, Spark ran out of memory
- The joins were just too expensive at this scale - I was fighting against Spark's execution model

I tried several optimizations: partitioning by join keys, time window filtering, strategic caching, more frequent checkpoints. They helped a bit, but the fundamental problem remained - iterative joins on 8M+ rows don't scale well in Spark.

**The breakthrough:**
I realized I could solve this in a single pass by processing trips chronologically. Instead of finding all predecessors upfront, I maintain a sliding window buffer organized by dropoff location. As I process each trip in chronological order, I look up potential predecessors from the buffer (trips that dropped off at the current pickup location within the 5-minute window), select the one with the maximum chain length, then add the current trip to the buffer for future lookups.

**Key design decisions:**
- **Single partition processing (`coalesce(1)`):** Critical for correctness - chains can span partition boundaries, and I need to process trips in strict chronological order. Multi-partition processing would miss cross-partition chains.
- **Location-indexed buffer:** Organized by DOLocationID for O(1) predecessor lookup. When processing a trip picking up at location L, I directly access buffer[L] to find candidates.
- **Memory-bounded buffer:** I clean up trips from the buffer whose dropoff time is outside the 5-minute window. Since I'm processing chronologically, any trip that dropped off more than 5 minutes before the current trip can never be a predecessor of future trips. This keeps memory usage bounded by trip density, not total dataset size.

**Why this works:**
- Single pass: O(N) time complexity instead of O(N × iterations)
- No query plan lineage issues: uses RDD `mapPartitions` directly, avoiding DataFrame join overhead
- Memory efficient: buffer typically holds thousands of trips (those in active 5-minute windows) rather than millions
- Correct: processes all trips globally in chronological order, finding chains that span the entire dataset

The algorithm uses dynamic programming: for each trip i, `chain_length[i] = 1 + max(chain_length[j])` where j is a valid predecessor. By processing chronologically, when I compute chain_length[i], all valid predecessors j have already been processed and their chain lengths are in the buffer.

```bash
sbt "runMain task4_chain.Task4LongestChain"
# Or with custom path:
sbt "runMain task4_chain.Task4LongestChain /path/to/your/data"
```
