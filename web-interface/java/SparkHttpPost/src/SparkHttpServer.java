/**
 * Created by nichozl on 15-5-21.
 */
import org.apache.spark.SparkEnv;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

class SparkHttpServer implements Serializable {
    private static final long serialVersionUID = 9121916089811464640l;
    final int RECEIVE_PORT = 9090;   // server port
    final String HDFS_PATH = "hdfs://head:9000/user/zhouliang/tmpinput/";   // hdfs path
    final String MASTER = "spark://head:7077";   // master
    final String TMPDIR = "/home/zhouliang/tmpfile/";
    //final String TMPDIR = "/home/nichozl/tmpfile/";
    //final String MASTER = "spark://localhost:7077";   // master

    public SparkHttpServer() {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        ServerSocket rServer = null;// ServerSocket instance
        final JavaSparkContext sc = new JavaSparkContext(MASTER, "Spark App");
        //final SQLContext sqlContext = new SQLContext(sc);
        try {
            rServer = new ServerSocket(RECEIVE_PORT);
            // ServerSocket init
            System.out.println("Welcome to the server!");
            System.out.println(new Date());
            System.out.println("The server is ready!");
            System.out.println("Port: " + RECEIVE_PORT);
            sc.addJar("SparkHttpPost.jar");
            //sc.addJar("./out/artifacts/SparkHttpPost_jar/SparkHttpPost.jar");
            while (true) {
                // wait for php info
                final Socket request = rServer.accept();// php client socket
                // receive the php client info
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        executeFunc(request, sc);
                    }
                });
            }

        } catch (Exception e) {
            executor.shutdown();
            System.out.println(e.getMessage());
        }
    }

    public static void main(String args[]) {
        new SparkHttpServer();
    }

    public void executeFunc(Socket threadPoolTaskData, JavaSparkContext sparkContext) {
        PrintWriter printer;
        FileWriter fileWriter;
        BufferedReader reader;
        String flag = "";
        try {
            fileWriter = new FileWriter(new File(TMPDIR+"tmp.txt"));
            printer = new PrintWriter(threadPoolTaskData.getOutputStream());
            reader = new BufferedReader(new InputStreamReader(threadPoolTaskData.getInputStream()));
            try {
                String phpPostStr = reader.readLine();

                phpPostStr = phpPostStr.replaceAll("==="," ");

                System.out.println("get information " + phpPostStr + "\n from " + threadPoolTaskData.getInetAddress().toString());

                //process the input string, delete the timestamp info
                String timestamp = phpPostStr.substring(phpPostStr.length()-19);
                phpPostStr = phpPostStr.substring(0,phpPostStr.length()-20);

                if(phpPostStr.substring(0,5).equals("start")) {
                    deleteDir(new File(TMPDIR + "tmp/" + timestamp + "/"));
                    printer.print("start");
                    printer.println();
                    printer.flush();
                    return;
                }

                if(phpPostStr.substring(0,4).equals("open")){
                    String[] notStr = phpPostStr.split("\\s+");
                    if (notStr.length > 2)
                    {
                        printer.print("exception");
                        printer.println();
                        printer.flush();
                        return;
                    }
                    reader = new BufferedReader(new FileReader(new File(TMPDIR + "tmp/" + timestamp + "/" + notStr[1])));
                    String line, fileStr="";
                    while ((line = reader.readLine()) != null) {
                        fileStr += line;
                    }
                    if (fileStr.substring(fileStr.length()-8).equals("complete"))
                    {
                        printer.print(fileStr.substring(0,fileStr.length()-9));
                    }
                    else {
                        printer.print("file not complete");
                    }
                    printer.println();
                    printer.flush();
                }
                else if (!phpPostStr.equals("exit")) {

                    String[] notStr = phpPostStr.split("\\s+");
                    String tablename = "";

                    for (int i = 1; i < notStr.length; i++) {
                        if (notStr[i].equals("from") || notStr[i].equals("FROM")) {
                            tablename = notStr[i+1];
                            break;
                        }
                    }
                    if (tablename.equals(""))
                    {
                        printer.print("exception");
                        printer.println();
                        printer.flush();
                        return;
                    }

                    flag = notStr[notStr.length-2];
                    if (flag.equals("&"))
                    {
                        phpPostStr = notStr[0];
                        for (int i = 1; i < notStr.length-2; i++) {
                            phpPostStr += " " + notStr[i];
                        }
                        File file = new File(TMPDIR + "tmp/" + timestamp);
                        file.mkdirs();
                        file = new File(TMPDIR + "tmp/" + timestamp + "/" + notStr[notStr.length-1]);
                        fileWriter = new FileWriter(file);
                        printer.print("ouput file");
                        printer.println();
                        printer.flush();
                    }

                    SQLContext sqlContext = new SQLContext(sparkContext);
                    SparkEnv.set(sparkContext.env());

                    //JavaRDD<DataPoint> pts = sparkContext.textFile("/home/nichozl/uploads/"+ tablename +".txt").map(
                    JavaRDD<DataPoint> pts = sparkContext.textFile(HDFS_PATH + tablename + ".txt").map(
                            new Function<String, DataPoint>() {
                                public DataPoint call(String line) throws Exception {
                                    String[] parts = line.split(" ");

                                    DataPoint pt = new DataPoint();
                                    pt.setNumber(parts[2]);
                                    pt.setX(Double.parseDouble(parts[0].trim()));
                                    pt.setY(Double.parseDouble(parts[1].trim()));
                                    return pt;
                                }
                            });

                    DataFrame schemaData = sqlContext.createDataFrame(pts, DataPoint.class);
                    schemaData.registerTempTable(tablename);
                    DataFrame res = sqlContext.sql(phpPostStr);

                    List<String> outputRes = res.toJavaRDD().map(new Function<Row, String>() {
                        public String call(Row row) {
                            String str = "";
                            for (int i = 0; i < row.size() - 1; i++) {
                                str = str + row.get(i) + ",";
                            }
                            str = str + row.get(row.size() - 1);
                            System.out.println(str);
                            return str;
                        }
                    }).collect();
                    if(outputRes.size()!=0) {
                        for (int i = 0; i < outputRes.size() - 1; i++) {
                            if (flag.equals("&")) {
                                fileWriter.write(outputRes.get(i) + " ");
                            }
                            else {
                                printer.print(outputRes.get(i) + " ");
                            }
                        }
                        if (flag.equals("&")) {
                            fileWriter.write(outputRes.get(outputRes.size() - 1)+" complete");
                        }
                        else {
                            printer.print(outputRes.get(outputRes.size() - 1));
                            printer.println();
                        }
                    }
                    else
                    {
                        if (flag.equals("&")) {
                            fileWriter.write("EmptySet complete");
                        }
                        else {
                            printer.print("EmptySet");
                            printer.println();
                        }
                    }
                    printer.flush();
                    fileWriter.close();
                }
                else {
                    fileWriter.close();
                    printer.close();
                    threadPoolTaskData.close();
                    System.out.println("php client leaving!\n");
                }
            } catch (Exception e) {
                if (flag.equals("&")) {
                    fileWriter.write("exception complete");
                    fileWriter.close();
                }
                else {
                    printer.print("exception");
                    printer.println();
                    printer.flush();
                }
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = (new File(dir, children[i])).delete();
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

    public static class DataPoint implements Serializable {

        private String number;
        private double x;
        private double y;

        public String getNumber() {
            return number;
        }

        public void setNumber(String number) {
            this.number = number;
        }

        public double getX() {
            return this.x;
        }

        public void setX(double x) {
            this.x = x;
        }

        public double getY() {
            return this.y;
        }

        public void setY(double y) {
            this.y = y;
        }
    }
}
