Simba
==========

This project aims at implementing a spatial database on Apache Spark, led by Associate Processor [Feifei Li](http://www.cs.utah.edu/~lifeifei) from [The University of Utah](http://www.utah.edu)  and Associate Processor [Bin Yao](http://www.cs.sjtu.edu.cn/~yaobin)  from [Shanghai Jiao Tong University](http://www.sjtu.edu.cn).

Features
--------------
+ distributed **partition** strategy & distributed **index**
+ efficient spatial **theta-join** operation
+ efficient **spatial query** operation
+ zepplin: **visualization** & **interactive** tool
+ basic Cost-Base Optimization(**CBO**)
+ **temporal** data and **textual** data analysis (developing)
+ **polygon** data storage & processing (developing)

Developer Notes
---------------

1. Checkout your own branch for your development, **DO NOT** push your draft version to the master branch
2. You build your own application in `org.apache.spark.examples` package for testing or debugging.
3. If you want to merge your feature branch to the main develop branch, please create a pull request from your local branch to develop branch (**not the master branch**).
4. Use IDE to debug this project. If you use IntelliJ IDEA, [INSTALL](./INSTALL) file contains a way to import the whole project to IntelliJ IDEA
