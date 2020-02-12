# perf-ops

`AWS` `S3` `Glue` `Presto` `Spark` `SparkSQL` `Alluxio` `Parquet` `TPC-H`

## Performance/Cost Optimizations and Benchmarks for Distributed SQL Engines

A comparative analysis of Distibuted SQL Engines `SparkSQL` and `Presto` , with and without `Alluxio`.

## Dataset and Spark ETL

- Dataset: [TPC-H](http://www.tpc.org/tpch/)

    <details>
    <summary>TPC-H Schema</summary>

  ![TPC-H Schema](tpch-schema-snowflake.png)

    </details>

  ![spark-etl](spark-etl.png)

* ETL Stats
  ![spark-etl-stats](spark-etl-stats.png)

  **7 node:** 1 x m4.xlarge(master), 6 x m4.large(worker nodes)

  **3 node:** 1 x m4.xlarge(master), 2 x m4.large(worker nodes)

## Architecture

- **Presto, Spark, Glue, S3**

  ![architecture.png](architecture.png)

## Results

![query-stats](query-stats.png)

## Queries

- q1
- q2

## Future Roadmap

- add the points from the slides

## Installations

<details>
  <summary>Presto</summary>
  
  ## Heading
  1. A numbered
  2. list
     * With some
     * Sub bullets
</details>

<details>
  <summary>Spark</summary>
  
  ## Heading
  1. A numbered
  2. list
     * With some
     * Sub bullets
</details>

<details>
  <summary>Glue</summary>
  
  ## Heading
  1. A numbered
  2. list
     * With some
     * Sub bullets
</details>

<details>
  <summary>Alluxio</summary>
  
  ## Heading
  1. A numbered
  2. list
     * With some
     * Sub bullets
</details>

## Additional Resources

- link to Slides (perf-ops)
- parquet
- presto paper link
- tpch
