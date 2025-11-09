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

## DataLoader Utility

The `DataLoader` utility is a shared component used across all tasks to avoid code repetition. It loads and merges Parquet files from a directory.

**Note:** All tasks save their output to text files in their respective task directories. For convenience, you can view these output files to see results without rerunning the tasks.

### Task 1: Data Exploration

Calculate the metrics/dimensions which, in your opinion, are the most suitable in order to understand and get familiar with the dataset.

```bash
sbt "runMain task1_exploration.Task1Exploration"
# Or with custom path:
sbt "runMain task1_exploration.Task1Exploration /path/to/your/data"
```

### Task 2: Top 10 Location Pairs (After Outlier Removal)

Remove trip_distance outliers from the dataset provided and calculate the top 10 PULocationId, DOLocationId pairs for total_amount. For outliers detection, you can use any algorithm that you think is suitable.

```bash
sbt "runMain task2_outliers.Task2TopPairs"
# Or with custom path:
sbt "runMain task2_outliers.Task2TopPairs /path/to/your/data"
```

### Task 3: Top 10 Location Pairs (Including Neighbors)

Calculate the same as in the previous step, but this time let's assume the pair includes values from neighbouring numbers, i.e. pair (5,5) includes (4,4), (4,5), (5,4), (5,5), (4,6), (6,4), (5,6), (6,5), (6,6)

```bash
sbt "runMain task3_neighbours.Task3NeighbourPairs"
# Or with custom path:
sbt "runMain task3_neighbours.Task3NeighbourPairs /path/to/your/data"
```

### Task 4: Longest Driver Chain

Let's assume that in the data, the same driver starts every trip in the same locationId as the previous one, and less than 5 minutes after drop-off; if these conditions are not met, it means the driver is different. Under this assumption, calculate the longest possible single-driver chain of trips from the provided dataset.

Uses a memory-efficient single-pass algorithm that processes trips chronologically with a sliding window buffer.

```bash
sbt "runMain task4_chain.Task4LongestChain"
# Or with custom path:
sbt "runMain task4_chain.Task4LongestChain /path/to/your/data"
```
