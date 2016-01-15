Web Demo
-------------------------------------------
This is a web demo module for SparkSpatial Project. It includes a php client and a java server project running as a Spark Application. You can follow the configuration below to build the web demo and use it.

### Configuration
You need to setup spark spatial environment and php environment before configuring the web demo.

* php web client

> Just copy the file directory `/path/to/sparkspatial/web/php/basic` to any place on your webspace.	  

* spark spatial conf

> Open the SparkHttpPost Project, and configure the hdfs path and the master in the file SparkHttpPost.java,

> ```       
> final String HDFS_PATH = "your hdfs path";   // hdfs path
> ```

> ```
> final String MASTER = "your master";   // master
> ```

> ```
> final String TMPDIR = "your self-defined filepath";   // directory to store the temporary file
> ```

* socket Port

> In this web demo, the default socket Port is 9090. If you want to change the default port, please follow the steps bellow:       

> Firstly, get into the `/path/to/sparkspatial/web/php/basic/views/site/` directory and open the file index.php and configure the clientsocket Port in this line,       

> ```
> $connect = socket_connect($socket, 'localhost', 9090);
> ```

> secondly, open the SparkHttpPost Project, and configure the serversocket Port in the file SparkHttpPost.java,

> ```       
> final int RECEIVE_PORT = 9090;   // server socket port
> ```

> notice that Port of the serversocket and clientsocket should be the same.

* import the lib directory

> You need to import the lib directory of the sparkspatial-distribution package to the project.

Finally, genenerate the SparkHttpPost.jar file in the directory `/path/to/sparkspatialweb/java/SparkHttpPost/out/artifacts/SparkHttpPost_jar/`. 
       
### Run
* you need to run the spark spatial cluster and your web server (such as Nginx, Apache) firstly.
   
* run the java server
      
> You can copy the jar file to the `/path/to/sparkspatial/` directory.  Then get into the directory and execute the jar file in the command line

> ```	  
> $ java -jar SparkHttpPost.jar
> ```

* enter the url `https://your webspace/basic/web/index.php` in your browser, then you can immediately use the web demo module for SparkSpatial Project