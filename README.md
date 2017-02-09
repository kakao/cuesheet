CueSheet
========

CueSheet is a framework for writing Apache Spark 2.x applications more conveniently, designed to neatly separate the concerns of the business logic and the deployment environment, as well as to minimize the usage of shell scripts which are inconvenient to write and do not support validation. To jump-start, check out [cuesheet-starter-kit](https://github.com/jongwook/cuesheet-starter-kit) which provides the skeleton for building CueSheet applications. CueSheet is featured in [Spark Summit East 2017](https://spark-summit.org/east-2017/events/no-more-sbt-assembly-rethinking-spark-submit-using-cuesheet/).

An example of a CueSheet application is shown below. Any Scala object extending `CueSheet` becomes a CueSheet application; the object body can then use the variables like `sc`, `sqlContext`, and `spark` to write the business logic, as if it is inside `spark-shell`:

```scala
import com.kakao.cuesheet.CueSheet

object Example extends CueSheet {{
  val rdd = sc.parallelize(1 to 100)
  println(s"sum = ${rdd.sum()}")
  println(s"sum2 = ${rdd.map(_ + 1).sum()}")
}}

```

CueSheet will take care of creating `SparkContext` or `SparkSession` according to the configuration given in a separate file, so that your application code can contain just the business logic. Furthermore, CueSheet will launch the application locally or to a YARN cluster by simply running your object as a Java application, eliminating the need to use `spark-submit` and accompanying shell scripts.

CueSheet also supports Spark Streaming applications, via `ssc`. When it is used in the object body, it automatically becomes a Spark Streaming application, and `ssc` provides access to the `StreamingContext`.

Importing CueSheet
---

<!-- DO NOT EDIT: The section below will be automatically updated by build script -->
```scala
libraryDependencies += "com.kakao.cuesheet" %% "cuesheet" % "0.10.0"
```
<!-- DO NOT EDIT: The section above will be automatically updated by build script -->

CueSheet can be used in Scala projects by configuring SBT as above. Note that this dependency is not specified as `"provided"`, which makes it possible to launch the application right in the IDE, and even debug using breakpoints in driver code when launched in client mode.

Configuration
---

Configurations for your CueSheet application, including [Spark configurations](http://spark.apache.org/docs/latest/configuration.html) and [the arguments in `spark-submit`](http://spark.apache.org/docs/latest/submitting-applications.html), are specified using [the HOCON format](https://github.com/typesafehub/config/blob/master/HOCON.md). It is by default `application.conf` in your classpath root, but [an alternate configuration file can be specified using `-Dconfig.resource` or `-Dconfig.file`](https://www.playframework.com/documentation/2.6.x/ProductionConfiguration#Specifying-an-alternate-configuration-file). Below is an example configuration file.

```
spark {
  master = "yarn:classpath:com.kakao.cuesheet.launcher.test"
  deploy.mode = cluster

  hadoop.user.name = "cloudera"

  executor.instances = 2
  executor.memory = 1g
  driver.memory = 1g

  streaming.blockInterval = 10000
  eventLog.enabled = false
  eventLog.dir = "hdfs:///user/spark/applicationHistory"
  yarn.historyServer.address = "http://history.server:18088"

  driver.extraJavaOptions = "-XX:MaxPermSize=512m"
}
```

Unlike the standard spark configuration, `spark.master` for YARN should include an indicator for finding YARN/Hive/Hadoop configurations. It is the easiest to put the XML files inside your classpath, usually by putting them under `src/main/resources`, and specify the package classpath as above. Alternatively, `spark.master` can contain a URL to download the configuration in a ZIP file, e.g. `yarn:http://cloudera.manager/hive/configuration.zip`, copied from Cloudera Manager's 'Download Client Configuration' link. The usual `local` or `local[8]` can also be used as `spark.master`.

`deploy.mode` can be either `client` or `cluster`, and `spark.hadoop.user.name` should be the username to be used as the Hadoop user. CueSheet assumes that this user has the write permission to the home directory.

## Using HDFS

While submitting an application to YARN, CueSheet will copy Spark and CueSheet's dependency jars to HDFS. This way, in the next time you submit your application, CueSheet will analyze your classpath to find and assemble only the classes that are not part of the already installed jars.

## One-Liner for Easy Deployment

When given a tag name as system property `cuesheet.install`, CueSheet will print a rather long shell command which can launch your application from anywhere `hdfs` command is available. Below is an example of the one-liner shell command that CueSheet produces when given `-Dcuesheet.install=v0.0.1` as a JVM argument.

```
rm -rf SimpleExample_2.10-v0.0.1 && mkdir SimpleExample_2.10-v0.0.1 && cd SimpleExample_2.10-v0.0.1 &&
echo '<configuration><property><name>dfs.ha.automatic-failover.enabled</name><value>false</value></property><property><name>fs.defaultFS</name><value>hdfs://quickstart.cloudera:8020</value></property></configuration>' > core-site.xml &&
hdfs --config . dfs -get hdfs:///user/cloudera/.cuesheet/applications/com.kakao.cuesheet.SimpleExample/v0.0.1/SimpleExample_2.10.jar \!SimpleExample_2.10.jar &&
hdfs --config . dfs -get hdfs:///user/cloudera/.cuesheet/lib/0.10.0-SNAPSHOT-scala-2.10-spark-2.1.0/*.jar &&
java -classpath "*" com.kakao.cuesheet.SimpleExample "hello" "world" && cd .. && rm -rf SimpleExample_2.10-v0.0.1
```

What this command does is to download the CueSheet and Spark jars as well as your application assembly from HDFS, and launch the application in the same environment that was launched in the IDE. This way, it is not required to have `HADOOP_CONF_DIR` or `SPARK_HOME` properly installed and set on every node, making it much easier to use it in distributed schedulers like Marathon, Chronos, or Aurora. These schedulers typically allow a single-line shell command as their job specification, so you can simply paste what CueSheet gives you in the scheduler's Web UI.


## Additional Features

Being started as a library of reusable Spark functions, CueSheet contains a number of additional features, not in an extremely coherent manner. Many parts of CueSheet including these features are powered by [Mango](https://github.com/kakao/mango) library, another open-source project by Kakao.

- [nearest-neighbor collaborative filtering](src/main/scala/com/kakao/cuesheet/cf/ItemBasedCF.scala)
- Connectors to [HBase, Couchbase](src/main/scala/com/kakao/cuesheet/convert/StringKeyRDD.scala), and [ElasticSearch](src/main/scala/com/kakao/cuesheet/convert/ElasticSearch.scala) to save RDD data with adjustable client-side throttling
- [Reading an HBase table as an RDD](src/main/scala/com/kakao/cuesheet/convert/HBaseReaders.scala)
- [Tools for parsing RDDs and DStreams encoded with Apache Avro](src/main/scala/com/kakao/cuesheet/convert/StringByteArrayRDD.scala)
- [An alternate join implementation for skewed dataset](src/main/scala/com/kakao/cuesheet/convert/JoinableRDD.scala)
- [Resumable Kafka Stream which reads ZooKeeper offset data instead of checkpoints](src/main/scala/com/kakao/cuesheet/convert/RichStreamingContext.scala) because checkpoint does not allow any changes in application code
- [Writing DataFrames into an external Hive table or partition](src/main/scala/com/kakao/cuesheet/convert/RichDataFrame.scala)

One additional quirk is the "stop" tab CueSheet adds to the Spark UI. As shown below, it features three buttons with an increasing degree of seriousness. To stop a Spark Streaming application, to possibly trigger a restart by a scheduler like Marathon, one of the left two buttons will do the job. If you need to halt a Spark application ASAP, the red button will immediately kill the Spark driver.

<img src="http://i.imgur.com/Ewqa6VB.png" alt="Stop Tab" width="600px">

## License

This software is licensed under the [Apache 2 license](LICENSE), quoted below.

Copyright 2017 Kakao Corp. <http://www.kakaocorp.com>

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this project except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
