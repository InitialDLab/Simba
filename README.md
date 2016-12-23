Simba: Spatial In-Memory Big data Analytics
===========================================
**Simba is now shipped as a standalone package outside Spark. Current version works with Spark 1.6.x. If you find any issues, please make a ticket in the issue tracking system.**

Simba is a distributed in-memory spatial analytics engine based on Apache Spark. It extends the Spark SQL engine across the system stack to support rich spatial queries and analytics through both SQL and the DataFrame API. Besides, Simba introduces native indexing support over RDDs in order to develop efficient spatial operators. It also extends Spark SQL's query optimizer with spatial-aware and cost-based optimizations to make the best use of existing indexes and statistics.

Simba is open sourced under Apache License 2.0. Currently, it is developed based on Spark 1.6.0. For recent updates and further information, please refer to [Simba's homepage](http://www.cs.utah.edu/~dongx/simba).

Features
--------------
+ Expressive **SQL and DataFrame query interface** fully *compatible with original Spark SQL operators*.  
  ***SQL mode is currently not supported in the standalone version.***
+ Native distributed **indexing** support over RDDs.
+ Efficient **spatial operators**: *high-throughput* & *low-latency*.
    - Box range query: `IN RANGE`
    - Circle range query: `IN CIRCLERANGE`
    - *k* nearest neighbor query: `IN KNN`
    - Distance join: `DISTANCE JOIN`
    - kNN join: `KNN JOIN`
+ Modified Zeppelin: **interactive visualization** for Simba.
+ Spatial-aware **optimizations**: *logical* & *cost-based*.
+ Native thread-pool for multi-threading.
+ **Geometric objects** support (developing)
+ **Spatio-Temporal** and **spatio-textual** data analysis (developing)

**Notes:** *We are still cleaning source codes for some of our features, which will be released to the master and develop branch later.*

Developer Notes
---------------
1. Fork this repo (or create your own branch if you are a member of Simba's main development team) to start your development, **DO NOT** push your draft version to the master branch
2. You can build your own application in `org.apache.spark.examples` package for testing or debugging.
3. If you want to merge your feature branch to the main develop branch, please create a pull request from your local branch to develop branch (**not the master branch**).
4. Use IDE to debug this project. If you use IntelliJ IDEA, [INSTALL](./INSTALL.md) file contains a way to import the whole project to IntelliJ IDEA

Branch Information
------------------
`standalone` branches are opened for maintaining Simba standalone package, which aims at building Simba packages standing outside Spark SQL core. Currently, `master` branch and `develop` branch are built on top of Spark 1.6.x. 

The `master` branch provides the latest stable version, while the `develop` branch is the main development branch where new features will be merged before ready to release. For legacy reasons, we also keep branches which archives old versions of Simba, which is developed based on former Spark versions, in the branches named `simba-spark-x.x`. Note that we will only integrate latest features into `master` and `develop` branches. Please make sure you checkout the correct branch before start using it.

Contributors
------------
- Dong Xie: dongx [at] cs [dot] utah [dot] edu
- Gefei Li: oizz01 [at] sjtu [dot] edu [dot] cn
- Liang Zhou: nichozl [at] sjtu [dot] edu [dot] cn
- Zhongpu Chen: chenzhongpu [at] sjtu [dot] edu [dot] cn
- Feifei Li: lifeifei [at] cs [dot] utah [dot] edu
- Bin Yao: yaobin [at] cs [dot] sjtu [dot] edu [dot] cn
- Minyi Guo: guo-my [at] cs [dot] sjtu [dot] edu [dot] cn
