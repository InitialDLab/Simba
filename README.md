Simba: Spatial In-Memory Big data Analytics
===========================================
Simba is a distributed in-memory spatial analytics engine based on Apache Spark. It extends the Spark SQL engine across the system stack to support rich spatial queries and analytics through both SQL and the DataFrame API. Besides, Simba introduces native indexing support over RDDs in order to develop efficient spatial operators. It also extends Spark SQL's query optimizer with spatial-aware and cost-based optimizations to make the best use of existing indexes and statistics.

Features
--------------
+ Expressive **SQL and DataFrame query interface** fully *compatible with original Spark SQL operators*.
+ Native distributed **indexing** support over RDDs.
+ Efficient **spatial operators**: *high-throughput* & *low-latency*.
  * Box range query: `IN RANGE`
  * Circle range query: `IN CIRCLERANGE`
  * *k* nearest neighbor query: `IN KNN`
  * Distance join: `DISTANCE JOIN`
  * kNN join: `KNN JOIN`
+ Modified Zeppelin: **interactive visualization** for Simba.
+ Spatial-aware **optimizations**: *logical* & *cost-based*.
+ **Spatio-Temporal** and **spatio-textual** data analysis (developing)
+ **Geometric objects** support (developing)

Developer Notes
---------------
1. Create your own branch for your development, **DO NOT** push your draft version to the master branch
2. You can build your own application in `org.apache.spark.examples` package for testing or debugging.
3. If you want to merge your feature branch to the main develop branch, please create a pull request from your local branch to develop branch (**not the master branch**).
4. Use IDE to debug this project. If you use IntelliJ IDEA, [INSTALL](./INSTALL.md) file contains a way to import the whole project to IntelliJ IDEA

Contributors
------------
- Dong Xie: dongx@cs.utah.edu
- Gefei Li: oizz01@sjtu.edu.cn
- Liang Zhou: nichozl@sjtu.edu.cn
- Feifei Li: lifeifei@cs.utah.edu
- Bin Yao: yaobin@cs.sjtu.edu.cn
