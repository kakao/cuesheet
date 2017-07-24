import scala.xml.transform.{RewriteRule, RuleTransformer}
import scala.xml.{Comment, Elem, Node => XmlNode, NodeSeq => XmlNodeSeq}
import UpdateReadme._

val sparkVersion = "2.2.0"

val hadoopVersion = "2.7.3"

val hbaseVersion = "1.3.0"

val hadoopDependencies = Seq("client", "yarn-api", "yarn-common", "yarn-server-web-proxy", "yarn-client")

val sparkDependencies = (Seq("core", "streaming", "mllib", "streaming-kafka-0-8", "hive", "yarn").map {
  dep => "org.apache.spark" %% s"spark-$dep" % sparkVersion excludeAll(hadoopDependencies.map{ dep => ExclusionRule("org.apache.hadoop", s"hadoop-$dep") }: _*)
} ++ hadoopDependencies.map {
  dep => "org.apache.hadoop" % s"hadoop-$dep" % hadoopVersion exclude("javax.servlet", "servlet-api")
}).map {
  // should leave Guava version be 14.0.1 (SPARK-6149)
  _ .exclude("com.google.guava", "guava")
    // prevent json4s pulling wrong scala libraries (https://github.com/json4s/json4s/pull/180)
    .exclude("org.scala-lang", "scalap")
    .exclude("org.scala-lang", "scala-library")
}

val hbaseDependencies = Seq("server", "common").map {
  dep => "org.apache.hbase" % s"hbase-$dep" % hbaseVersion exclude("org.mortbay.jetty", "servlet-api-2.5")
}

val jdkVersionCheck = taskKey[Unit]("Check JDK version")

jdkVersionCheck := {
  val required = "1.7"
  val current  = sys.props("java.specification.version")
  assert(current == required || sys.env.contains("TRAVIS"), s"JDK $required is required for compatibility; current version = $current")
}

val settings = Seq(
  organization := "com.kakao.cuesheet",
  isSnapshot := version.value.endsWith("-SNAPSHOT"),
  crossScalaVersions := Seq("2.10.6", "2.11.8"),
  scalaVersion := crossScalaVersions.value.last,
  // disable Scaladoc for now
  sources in (Compile, doc) := Seq.empty,
  scalacOptions := Seq("-feature", "-unchecked", "-encoding", "utf8"),

  // publish configurations
  homepage := Some(url("https://github.com/kakao/cuesheet")),
  licenses := Seq("The Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  description := "A framework for writing Spark 2.x applications, in a pretty way",
  scmInfo := Some { val git = "https://github.com/kakao/cuesheet.git"; ScmInfo(url(git), s"scm:git:$git", Some(s"scm:git:$git")) },
  developers := List(Developer("jongwook", "Jong Wook Kim", "jongwook@nyu.edu", url("https://github.com/jongwook"))),
  publishTo := {
    val maven = "https://oss.sonatype.org"
    if (isSnapshot.value)
      Some("Sonatype Snapshots" at s"$maven/content/repositories/snapshots")
    else
      Some("Sonatype Staging" at s"$maven/service/local/staging/deploy/maven2")
  },

  // add Sonatype credentials if it exists
  credentials ++= Seq(Path.userHome / ".ivy2" / ".credentials").filter(_.exists()).map(Credentials.apply),
  // add updateReadme action to the release process
  releaseProcess := customReleaseProcess,
  // codesign artifacts
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  // cross-build on release by default
  releaseCrossBuild := true,
  // remove provided and test scope dependency from the POM
  pomPostProcess := { (node: XmlNode) =>
    new RuleTransformer(new RewriteRule {
      override def transform(node: XmlNode): XmlNodeSeq = node match {
        case e: Elem if e.label == "dependency" && (e \ "scope").map(_.text).exists(Seq("provided", "test").contains) =>
          val organization = (e \ "groupId").head.text
          val artifact = (e \ "artifactId").head.text
          val version = (e \ "version").head.text
          val scope = (e \ "scope").head.text

          Comment(s"$scope dependency $organization#$artifact;$version has been omitted")
        case _ => node
      }
    }).transform(node).head
  },
  (packageBin in Compile) <<= (packageBin in Compile) dependsOn jdkVersionCheck
)

val root = Project("cuesheet", base = file(".")).settings(settings ++ Seq(
  libraryDependencies ++= sparkDependencies ++ hbaseDependencies ++ Seq(
    "com.kakao.mango" %% "mango" % "0.5.0",
    "com.github.nscala-time" %% "nscala-time" % "2.16.0",
    "org.apache.hbase" % "hbase-common" % hbaseVersion,
    "org.eclipse.jetty" % s"jetty-servlet" % "9.2.16.v20160414" % "provided",  // needed to extend WebUI in Scala 2.10
    "log4j" % "log4j" % "1.2.17" % "test",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
))
