# Spark Architecture

## Key Features: 
- Sparks Start page: https://spark.apache.org/
- PySpark APIs: https://spark.apache.org/docs/latest/api/python/index.html
- https://data-flair.training/blogs/what-is-spark/

## Spark vs. Hadoop: 
- https://www.codeconquest.com/blog/spark-vs-hadoop-mapreduce-performance-and-resource-management/#:~:text=Generally%20speaking%2C%20Spark%20is%20faster,10%20times%20faster%20on%20disk
- Performance Spark vs. Hadoop: https://spark.apache.org/news/spark-wins-daytona-gray-sort-100tb-benchmark.html
- https://www.ibm.com/blog/hadoop-vs-spark/

## DAG and Catalyst Optimizer

- DAG: https://medium.com/@ashutoshkumar2048/dag-in-apache-spark-a3fee17f7494#:~:text=In%20Apache%20Spark%2C%20DAG%20stands,in%20an%20e%2Dcommerce%20company.
- Catalyst Optimizer: https://www.databricks.com/glossary/catalyst-optimizer
- Catalyst Optimizer: https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html
- DAG and Catalyst Optimizer: https://medium.com/@badwaik.ojas/short-note-on-dag-and-catalyst-optimizer-in-apache-spark-ecaa9a4c2a82

## Memory Management:
- https://pawankg.medium.com/exploring-pyspark-memory-management-resource-control-and-database-interactions-327c24977895

# Concurrent and Asynchronues programming

## Threading
- https://superfastpython.com/python-use-all-cpu-cores/#Python_Will_Not_Use_All_CPUs_By_Default

## Asyncio
- https://realpython.com/async-io-python/#setting-up-your-environment


## Asyncio vs Threading
- https://www.dataleadsfuture.com/combining-traditional-thread-based-code-and-asyncio-in-python/

## Asyncio and Threading together
- https://docs.python.org/3.10/library/asyncio-eventloop.html#asyncio.loop.run_in_executor
- https://stackoverflow.com/questions/28492103/how-to-combine-python-asyncio-with-threads

# APIs
- HTTPX

# Parquet and Spark
- https://dennyglee.com/2024/02/13/how-apache-spark-performs-a-fast-count-using-the-parquet-metadata/
- https://www.linkedin.com/posts/dennyglee_how-apache-spark-performs-a-fast-count-using-activity-7163200117175447552-GVh6/?utm_source=share&utm_medium=member_android
- https://github.com/dennyglee/databricks/blob/master/misc/parquet-count-metadata-explanation.md
- https://learncsdesigns.medium.com/understanding-apache-parquet-d722645cfe74
- Parquet and other data source formats: https://www.youtube.com/watch?v=auNAzC3AU18
- https://medium.com/@diehardankush/predicate-pushdown-in-parquet-enhancing-efficiency-and-performance-5becb0b992de#:~:text=At%20its%20core%2C%20predicate%20pushdown,the%20database%20query%20execution%20plan
- https://medium.com/@vladimir.prus/spark-partitioning-the-fine-print-5ee02e7cb40b
- https://stackoverflow.com/questions/76782018/what-is-actually-meant-when-referring-to-parquet-row-group-size
- https://www.gresearch.com/blog/article/parquet-files-know-your-scaling-limits/

# UDFs
- GroupBy for concurrency: https://github.com/jamesshocking/Spark-REST-API-UDF

# Delta Lake

## General
- https://delta.io/blog/delta-lake-3-1/
- https://delta.io/blog/delta-lake-vs-parquet-comparison/

## Liquid Clustering
- Liquid Clustering: https://medium.com/@stevejvoros/liquid-clustering-an-innovative-approach-to-data-layout-in-delta-lake-1a277f57af99
- https://www.linkedin.com/posts/dennyglee_from-partitioning-to-liquid-clustering-activity-7161395777225850880-S9bQ/?utm_source=share&utm_medium=member_android
- https://dennyglee.com/2024/02/06/how-delta-lake-liquid-clustering-conceptually-works/

## Delta Log
- https://dennyglee.com/2023/08/31/what-is-the-delta-lake-transaction-log/

# Partitions and Repartitioning
- https://stackoverflow.com/questions/65809909/spark-what-is-the-difference-between-repartition-and-repartitionbyrange
- https://www.projectpro.io/article/how-data-partitioning-in-spark-helps-achieve-more-parallelism/297#:~:text=Range%20Partitioning%20in%20Spark,-Some%20Spark%20RDDs&text=In%20range%20partitioning%20method%2C%20tuples,keys%20and%20ordering%20of%20keys
- https://stackoverflow.com/questions/70985235/what-is-opencostinbytes
- https://vivekjadhavr.medium.com/understanding-and-configuring-partition-size-in-apache-spark-3889ecb5259a#:~:text=openCostInBytes%3A%20The%20openCostInBytes%20parameter%20defines,4%20MB%20
- https://chengzhizhao.com/uncovering-the-truth-about-apache-spark-performance-coalesce1-vs-repartition1/