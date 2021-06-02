# Documentation

The goal of this task is to implement an application to process electrical energy consumption data in order to deliver insights into the electricity usage in a building.

## Metrics

I considered the following set of metrics;

- energy_consumption: the total energy consumed by a meter during 1 hour
- min_power: minimum power reading during each hour per meter
- mean_power: mean power reading during each hour per meter
- mean_power: mean power reading during each hour per meter
- 3rd_quartile_power: total number of readings per meter during 1 hour
- reading_per_hour: total number of readings per meter during 1 hour

## Design consideration

Keeping in that there are several meters and reading per hour. Let us assume we have `1000 meters` with average of `100` readings per hour per meter. We have `100k` reading in total per hour for all meters and `2.4m` readings per day for all meters.

In order to handle such a huge amount of data processing, I use `Apache Spark` for data processing because of its capability to process billions of record, and it would be easy to scale if the number of meter increases for example. Aggregated data (processed data) is saved in `.csv` format. We could also stream aggregated result to `Kafka` for real-time monitoring and further processing.

## Minimum requirements

The application is built and tested with Apache Spark using Scala api.

- `Apache Spark v3.0`
- `Scala v2.12`
- `sbt`
- `Java 8`

## Usage

- unzip file
- in the current directory, open a terminal
- build an uber-jar with `sbt assembly` and wait a few minutes to build (probably grab a coffee)
- confirm there is "ProjectDir/target/scala-2.12/RecommendationEngine-assembly-0.1.jar"
- run the following command to start the spark application

> Run the following code;

```bash
spark-submit \
--class fr.squaresense.eec.Main \
--master local \
/ProjectDir/target/scala-2.12/EECDataProcessing-assembly-0.1.jar /ProjectDir/src/main/scala/resources/test-data.csv

```

## Testing

Run `sbt test` from project directory