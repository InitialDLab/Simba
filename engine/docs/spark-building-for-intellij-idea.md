Spark Building & Importing Spark into IntelliJ IDEA
====================================================
This document shows how to build spark with maven, how to run unit test and how to import the project into IntelliJ IDEA.

Dependency
----------
- Java 1.6+ (1.7 recommended)
- Scala 2.10.x (Note that 2.11.x may not work)
- IntelliJ IDEA 14
- Maven 3.0.4+
- Source code of Spark 1.3

Build Spark and Run Unit Test
-----------------------------
Spark now comes packaged with a self-contained Maven installation to ease building and deployment of Spark from source located under the build/ directory. This script will automatically download and setup all necessary build requirements (Maven, Scala, and Zinc) locally within the `build` directory itself. It honors any mvn binary if present already, however, will pull down its own copy of Scala and Zinc regardless to ensure proper version requirements are met. build/mvn execution acts as a pass through to the mvn call allowing easy transition from previous build methods. As an example, one can build a version of Spark as follows:

    build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests clean package

You鈥檒l need to configure Maven to use more memory than usual by setting MAVEN_OPTS. We recommend the following settings:

    export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

If you don鈥檛 run this, you may see errors like the following:
    
    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-2.10/classes...
    [ERROR] PermGen space -> [Help 1]

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-2.10/classes...
    [ERROR] Java heap space -> [Help 1]

You can fix this by setting the MAVEN_OPTS variable as discussed before.

**Note:** *For Java 8 and above this step is not required. If using build/mvn and MAVEN_OPTS were not already set, the script will automate this for you.*

Tests are run by default via the ScalaTest Maven plugin.
Some of the tests require Spark to be packaged first, so always run `mvn package` with `-DskipTests` the first time. The following is an example of a correct (build, test) sequence:

    mvn -Pyarn -Phadoop-2.4 -DskipTests -Phive -Phive-thriftserver clean package
    mvn -Pyarn -Phadoop-2.4 -Phive -Phive-thriftserver test

The ScalaTest plugin also supports running only a specific test suite as follows:

    mvn -Dhadoop.version=... -DwildcardSuites=org.apache.spark.repl.ReplSuite test

Importing Spark into IntelliJ IDEA
----------------------------------
While many of the Spark developers use SBT or Maven on the command line, the most common IDE we use is IntelliJ IDEA. You can get the community edition for free (Apache committers can get free IntelliJ Ultimate Edition licenses) and install the JetBrains Scala plugin from File > Settings > Plugins.

To create a Spark project for IntelliJ:

1.  Download IntelliJ and install the [Scala plug-in for IntelliJ](https://confluence.jetbrains.com/display/SCA/Scala+Plugin+for+IntelliJ+IDEA).
2.  Modify `./spark-1.3.0/external/flume-sink/pom.xml` to the  new version(this has been done in the source code)
3.  Go to File > New > Project from Existing Sources (Or just click on Import Project on the welcome page) to start importing. Locate the spark source directory, select Import project from external model and select Maven.
4.  In the Import wizard, it's fine to leave settings at their default. However it is usually useful to enable "Import Maven projects automatically", since changes to the project structure will automatically update the IntelliJ project.
5.  Go to View > Tool Windows > Maven Projects and add `hadoop-2.4`, `hive-0.12.0`, `hive-thriftserver`, `yarn` in `Profiles`. Then, reimport all maven projects (press the first button on upper-left corner) and generate sources and update folders for all projects (press the second button on upper-left corner).
6.  Go to File > Project Structure > Project Settings > Modules. Find spark-hive_2.10 and spark-hive-thriftserver_2.10, mark `./v0.12.0/src/main/scala` in each module as sources. (Click on the Sources right to Mark as)
7.  Go to Build > Rebuild Project to start building the project.

References
----------
- [Building Spark](http://spark.apache.org/docs/latest/building-spark.html)
- [Contributing to Spark](https://cwiki.apache.org/confluence/display/SPARK/Contributing+to+Spark#ContributingtoSpark-IntelliJ)
- [Fix Importing Issues in flume-sink](http://apache-spark-developers-list.1001551.n3.nabble.com/A-Spark-Compilation-Question-td8402.html)
- [Fix Importing Issues in hive and hivethriftserver](http://stackoverflow.com/questions/28957861/cannot-build-spark-in-intellij-14)
