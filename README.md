# perf-ops

`AWS` `S3` `Glue` `Presto` `Spark` `SparkSQL` `Alluxio` `Parquet` `TPC-H`

## Performance Optimizations Analysis for Distributed SQL Engines

A comparative analysis of Distibuted SQL Engines `SparkSQL` and `Presto`

## Dataset and Spark ETL

- Dataset: [TPC-H](http://www.tpc.org/tpch/)

    <details>
    <summary>TPC-H Schema</summary>

  ![TPC-H Schema](./assets/tpch-schema-snowflake.png)

  [source](https://docs.snowflake.net/manuals/user-guide/sample-data-tpch.html)

    </details>

  ![spark-etl](./assets/spark-etl.png)

  <details>
  <summary>ETL Stats</summary>

  ![spark-etl-stats](./assets/spark-etl-stats.png)

  **7 node:** 1 x m4.xlarge(master), 6 x m4.large(worker nodes)

  **3 node:** 1 x m4.xlarge(master), 2 x m4.large(worker nodes)

  </details>

## Architecture

- **Presto, Spark, Glue, S3**

  ![architecture.png](./assets/architecture.png)

## Results

- **Query Stats**

  ![query-stats](./assets/query-stats.png)

- **Queries**

  <details>
  <summary>Q1</summary>

  ![q1](./assets/q1.png)

  </details>

  <details>
  <summary>Q2</summary>

  ![q2](./assets/q2.png)

  </details>

  <details>
  <summary>Q3</summary>

  ![q3](./assets/q3.png)

  </details>

## Future Roadmap

- Automate the cluster provisioning

- Automate experiments and stats collection

  - Config driven framework

- Run federated queries

## Additional Resources

- [Slides](http://bit.ly/perf-ops-slides)
- [Presto paper](https://prestosql.io/Presto_SQL_on_Everything.pdf)
