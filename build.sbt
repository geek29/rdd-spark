import AssemblyKeys._ // put this at the top of the file

name := "rdd-spark"

version := "1.0"

scalaVersion := "2.10.4"

//sbtVersion := "0.13.5"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
 
resolvers += "akka" at "http://repo.akka.io/snapshots"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.3.6",
  "com.typesafe.akka" %% "akka-remote" % "2.3.6",
  "com.typesafe.akka" %% "akka-slf4j" % "2.3.6",
  "org.scalatest" % "scalatest_2.10" % "2.2.1" % "test",
  "junit" % "junit" % "4.10" % "test",
  "com.novocode" % "junit-interface" % "0.10" % "test",
  "com.google.code.gson" % "gson" % "2.3",
  "com.jayway.jsonpath" % "json-path" % "1.2.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.4",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.3.4",    
  "org.apache.spark" %% "spark-core" % "1.2.0"     
)

//"org.apache.hadoop" % "hadoop-common" % "2.4.1", 
//"org.apache.hadoop" % "hadoop-hdfs" % "2.4.1", 
//"org.apache.hadoop" % "hadoop-client" % "2.4.1",
//assemblySettings

//unmanagedBase := baseDirectory.value / "custom_lib"

//excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
//  cp filter { _.data.getName == "gemfire.jar"}
//}

//aggregate in package := false


