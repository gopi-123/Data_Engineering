# Data_Engineering
Projects and Notes



### ADLS storage account


#### Differnece between Pandas vs Pyspark:
Pandas and PySpark are both popular libraries in the Python ecosystem for data manipulation and analysis, but they have different use cases and trade-offs. Here are some key differences between them:

Data Scale:

Pandas: Designed to work with data that fits into memory on a single machine. It operates primarily on a single node and is best suited for datasets that are not excessively large.
PySpark: Built for distributed data processing and is capable of handling very large datasets that exceed the memory capacity of a single machine. It can distribute data across a cluster of machines and perform operations in parallel.
Execution Model:

Pandas: Executes operations in-memory on a single machine using a single-threaded or multi-threaded approach, depending on how it's configured.
PySpark: Executes operations in a distributed manner across a cluster of machines. It uses a distributed computing framework (Apache Spark) to parallelize data processing tasks.
API and Syntax:

Pandas: Offers a DataFrame API that is similar to working with tables in a relational database or working with data in Excel. It provides a rich set of functions and methods for data manipulation, exploration, and analysis.
PySpark: Also provides a DataFrame API, which is inspired by Pandas but has some differences in syntax and functionality. PySpark DataFrame operations are designed to work in a distributed environment and are optimized for parallel processing.
Performance:

Pandas: Generally faster for small to medium-sized datasets that fit into memory on a single machine. It's optimized for single-node processing and can leverage optimizations like vectorized operations for CPU-bound tasks.
PySpark: Excels in handling very large datasets and can scale out to multiple machines. It's optimized for distributed processing and can efficiently handle CPU-bound and memory-bound tasks by parallelizing computation across a cluster.
Deployment Complexity:

Pandas: Simple to install and use since it's a Python library that can be installed via pip. However, scaling Pandas to handle large datasets may require additional infrastructure or optimization efforts.
PySpark: Requires a Spark cluster to be set up, which adds complexity to deployment and maintenance. However, once the cluster is set up, PySpark provides a scalable and distributed computing environment out of the box.
In summary, Pandas is well-suited for small to medium-sized datasets and interactive data analysis tasks on a single machine, while PySpark is designed for large-scale data processing and analysis in distributed environments. The choice between them depends on the scale of your data, the complexity of your processing tasks, and the infrastructure available for deployment.
