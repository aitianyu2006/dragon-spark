name := "dragon-spark"

scalaVersion := "2.10.4"

organization := "com.remarkmedia.dragon.spark"

version := "0.1"

resolvers in ThisBuild += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

resolvers in ThisBuild += "mvnrepository" at "http://mvnrepository.com/"


libraryDependencies ++= {
  val hbaseV = "1.0.0-cdh5.5.1"
  val sparkV = "1.5.0-cdh5.5.1"
  Seq(
    ("org.apache.hbase" % "hbase-common" % hbaseV).exclude("commons-logging", "commons-logging").
    exclude("com.typesafe.play", "sbt-link").exclude("org.mortbay.jetty", "servlet-api-2.5").
      exclude("org.mortbay.jetty", "jetty-util").
      exclude("org.mortbay.jetty", "jetty").
      exclude("tomcat", "jasper-compiler").exclude("org.mortbay.jetty","jsp-2.1").exclude("tomcat","jasper-runtime"),
    ("org.apache.hbase" % "hbase-client" % hbaseV).exclude("commons-logging", "commons-logging").
      exclude("com.typesafe.play", "sbt-link").exclude("org.mortbay.jetty", "servlet-api-2.5").
      exclude("org.mortbay.jetty", "jetty-util").
      exclude("org.mortbay.jetty", "jetty").
      exclude("tomcat", "jasper-compiler").exclude("org.mortbay.jetty","jsp-2.1").exclude("tomcat","jasper-runtime"),
    ("org.apache.hbase" % "hbase-server" % hbaseV).exclude("commons-logging", "commons-logging").
      exclude("com.typesafe.play", "sbt-link").exclude("org.mortbay.jetty", "servlet-api-2.5").
      exclude("org.mortbay.jetty", "jetty-util").
      exclude("org.mortbay.jetty", "jetty").
      exclude("tomcat", "jasper-compiler").exclude("org.mortbay.jetty","jsp-2.1").exclude("tomcat","jasper-runtime"),
    "org.apache.spark" % "spark-streaming-kafka_2.10" % sparkV % "provided",
    "org.apache.spark" % "spark-core_2.10" % sparkV % "provided",
    "org.apache.spark" % "spark-streaming_2.10" % "1.5.0-cdh5.5.1" % "provided",
    "org.apache.spark" % "spark-streaming_2.10" % "1.5.0-cdh5.5.1" % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % "1.5.0-cdh5.5.1" % "provided",
    "org.json4s" %% "json4s-jackson" % "3.2.11",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1",
    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.1",
    "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.1"

  )

//libraryDependencies ++= {
//  val hbaseV = "1.0.0-cdh5.5.1"
//  val sparkV = "1.5.0-cdh5.5.1"
//  Seq(
//    ("org.apache.hbase" % "hbase-common" % hbaseV).exclude("commons-logging", "commons-logging").
//    exclude("com.typesafe.play", "sbt-link").exclude("org.mortbay.jetty", "servlet-api-2.5").
//      exclude("org.mortbay.jetty", "jetty-util").
//      exclude("org.mortbay.jetty", "jetty").
//      exclude("tomcat", "jasper-compiler").exclude("org.mortbay.jetty","jsp-2.1").exclude("tomcat","jasper-runtime"),
//    ("org.apache.hbase" % "hbase-client" % hbaseV).exclude("commons-logging", "commons-logging").
//      exclude("com.typesafe.play", "sbt-link").exclude("org.mortbay.jetty", "servlet-api-2.5").
//      exclude("org.mortbay.jetty", "jetty-util").
//      exclude("org.mortbay.jetty", "jetty").
//      exclude("tomcat", "jasper-compiler").exclude("org.mortbay.jetty","jsp-2.1").exclude("tomcat","jasper-runtime"),
//    ("org.apache.hbase" % "hbase-server" % hbaseV).exclude("commons-logging", "commons-logging").
//      exclude("com.typesafe.play", "sbt-link").exclude("org.mortbay.jetty", "servlet-api-2.5").
//      exclude("org.mortbay.jetty", "jetty-util").
//      exclude("org.mortbay.jetty", "jetty").
//      exclude("tomcat", "jasper-compiler").exclude("org.mortbay.jetty","jsp-2.1").exclude("tomcat","jasper-runtime"),
//    "org.apache.spark" % "spark-streaming-kafka_2.10" % sparkV ,
//    "org.apache.spark" % "spark-core_2.10" % sparkV ,
//    "org.apache.spark" % "spark-streaming_2.10" % "1.5.0-cdh5.5.1",
//    "org.apache.spark" % "spark-streaming_2.10" % "1.5.0-cdh5.5.1" ,
//    "org.apache.spark" % "spark-mllib_2.10" % "1.5.0-cdh5.5.1" ,
//    "org.json4s" %% "json4s-jackson" % "3.2.11",
//    "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.1",
//    "com.fasterxml.jackson.core" % "jackson-core" % "2.6.1",
//    "com.fasterxml.jackson.core" % "jackson-annotations" % "2.6.1"
//
//  )



}









