name := "Daffodil Spark Test"

version := "0.1.1"

scalaVersion := "2.11.12"

retrieveManaged := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0" % "test",
  "org.apache.daffodil" %% "daffodil-sapi" % "2.2.0" % "test",
  "junit" % "junit" % "4.12" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test"
)

