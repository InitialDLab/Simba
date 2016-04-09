Simba
==========

This project aims at implementing a spatial database on Apache Spark, led by Associate Processor [Feifei Li](http://www.cs.utah.edu/~lifeifei) from [The University of Utah](http://www.utah.edu)  and Associate Processor [Bin Yao](http://www.cs.sjtu.edu.cn/~yaobin)  from [Shanghai Jiao Tong University](http://www.sjtu.edu.cn).

Dependency
----------
- Java 1.6+ (1.7 or 1.8 recommended)
- Scala 2.10.x (Note that 2.11.x may not work)
- IntelliJ IDEA
- Maven 3.3.3+
- Source code of Simba

Build Simba and Run Unit Test
-----------------------------
Simba now comes packaged with a self-contained Maven installation to ease building and deployment of Simba from source located under the build/ directory. This script will automatically download and setup all necessary build requirements (Maven, Scala, and Zinc) locally within the `build` directory itself. It honors any mvn binary if present already, however, will pull down its own copy of Scala and Zinc regardless to ensure proper version requirements are met. build/mvn execution acts as a pass through to the mvn call allowing easy transition from previous build methods. As an example, one can build a version of Simba as follows:
```
build/mvn -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.2 -DskipTests clean package
```
You will need to configure Maven to use more memory than usual by setting MAVEN_OPTS. We recommend the following settings:
```
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```
If you don't run this, you may see errors like the following:
```
[INFO] Compiling 203 Scala sources and 9 Java sources to /path/to/simba/core/target/scala-2.10/classes...
[ERROR] PermGen space -> [Help 1]

[INFO] Compiling 203 Scala sources and 9 Java sources to /path/to/simba/core/target/scala-2.10/classes...
[ERROR] Java heap space -> [Help 1]
```
You can fix this by setting the MAVEN_OPTS variable as discussed before.

**Note:** *For Java 8 and above this step is not required. If using build/mvn and MAVEN_OPTS were not already set, the script will automate this for you.*

Tests are run by default via the ScalaTest Maven plugin.
Some of the tests require Simba to be packaged first, so always run `mvn package` with `-DskipTests` the first time. The following is an example of a correct (build, test) sequence:
```
mvn -Pyarn -Phadoop-2.6 -DskipTests -Phive -Phive-thriftserver clean package
mvn -Pyarn -Phadoop-2.6 -Phive -Phive-thriftserver test
```
The ScalaTest plugin also supports running only a specific test suite as follows:
```
mvn -Dhadoop.version=... -DwildcardSuites=org.apache.spark.repl.ReplSuite test
```
Importing Simba into IntelliJ IDEA
----------------------------------
While many of the Simba developers use SBT or Maven on the command line, the most common IDE we use is IntelliJ IDEA. You can get the community edition for free (Apache committers can get free IntelliJ Ultimate Edition licenses) and install the JetBrains Scala plugin from File > Settings > Plugins.

To create a Simba project for IntelliJ:

1.  Download IntelliJ and install the [Scala plug-in for IntelliJ](https://confluence.jetbrains.com/display/SCA/Scala+Plugin+for+IntelliJ+IDEA).
2.  Go to File > New > Project from Existing Sources (Or just click on Import Project on the welcome page) to start importing. Locate the Simba source directory, select Import project from external model and select Maven.
3.  In the Import wizard, it's fine to leave settings at their default. However it is usually useful to enable "Import Maven projects automatically", since changes to the project structure will automatically update the IntelliJ project.
4.  Go to View > Tool Windows > Maven Projects and add `hadoop-2.6`, `hive-provided`, `hive-thriftserver`, `yarn` in `Profiles`. Then, reimport all maven projects (press the first button on upper-left corner) and generate sources and update folders for all projects (press the second button on upper-left corner).
5.  Go to File > Project Structure > Project Settings > Modules. Find `spark-streaming-flume-sink`, mark `target/scala-2.10/src_managed/main/compiled_avro` as source. (Click on the Sources on the top to mark)
6.  Go to Build > Rebuild Project to start building the project.

Build Simba Release
-------------------
- Setting up Maven's Memory Usage
```
export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"
```
- Build Simba project
```
cd /path/to/simba/
./make-distribution.sh --skip-java-test --tgz --mvn mvn -Phadoop-2.4 -Pyarn -Dhadoop.verison=2.4.1 -DskipTests
```
* The output distribution of Simba is in the directory `/path/to/simba/dist/`.

Simba Environment Setup
-----------------------
- Please first set up SSH connections without password for the whole cluster.

- Edit `/path/to/simba/conf/slaves`, add the hostnames of slaves to this file:
```
Slave1
Slave2
```
- Edit `/path/to/simba/conf/spark-env.sh`, add the necessary environment variables to the file (you can refer to `/path/to/simba/conf/spark-env.sh.template`):
```
export SCALA_HOME=/env/scala-2.10.6
export HADOOP_HOME=/env/hadoop-2.6.2/
export JAVA_HOME=/env/jdk1.7.0_79
export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
export SPARK_MASTER_IP=Master   # your master node IP address or hostname
export SPARK_WORKER_CORES=8
export SPARK_WORKER_MEMORY=5g
```
- Add the necessary environment variables to your `~/.bashrc`:
```
export SIMBA_HOME=/path/to/simba
export PATH=$PATH:$SIMBA_HOME/bin
```
- Use `scp` to distribute Simba binary with all above configurations to all other nodes in the cluster.

- Run the following command on your master node to start daemon of Simba:
```
cd /path/to/simba/
./sbin/start-all.sh
```
###**Developer Notes**

1. Checkout your own branch for your development, **DO NOT** push your draft version to the master branch
2. You build your own application in `org.apache.spark.examples` package for testing or debugging.
3. If you want to merge your feature branch to the main develop branch, please create a pull request from your local branch to develop branch (**not the master branch**).
