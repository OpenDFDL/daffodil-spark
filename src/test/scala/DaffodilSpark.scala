/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.daffodil.sapi.Daffodil
import java.nio.channels.Channels
import scala.collection.mutable
import org.apache.daffodil.sapi.Daffodil
import org.apache.daffodil.sapi.io.InputSourceDataInputStream
import org.apache.daffodil.sapi.infoset.ScalaXMLInfosetOutputter
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.ByteArrayOutputStream
import java.io.ByteArrayInputStream
import scala.collection.JavaConverters._
import java.net.URI
import org.apache.daffodil.sapi.DataProcessor
import java.io.IOException
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object DaffodilSpark {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("JOB")

  //
  // This is just to avoid re-reloading the data processor for every file
  // parsed. It should no longer be necessary once we get rid of use of the dpBytes
  // I.e., when https://issues.apache.org/jira/browse/DAFFODIL-1440 is fixed.
  //
  private var dp_ : DataProcessor = null
  def getDP(dpBytes: Array[Byte]) = {
    if (dp_ ne null) dp_
    else {
      dp_ = Daffodil.compiler().reload(Channels.newChannel(new ByteArrayInputStream(dpBytes)))
      dp_
    }
  }

  def main(args: Array[String]): Unit = {

    // create a Daffodil DataProcessor using arg(0) as the schema. This
    // DataProcessor is used by the mapping functions, and so Spark send
    // it to the clusters, which will then use it in the map steps.

    val c = Daffodil.compiler()
    val pf = c.compileSource(new URI(args(0)))
    if (pf.isError) {
      pf.getDiagnostics.foreach { d => System.err.println(d.getMessage()) }
      throw pf.getDiagnostics(0).getSomeCause
    }
    val dp = pf.onPath("/")
    if (dp.isError) {
      dp.getDiagnostics.foreach { d => System.err.println(d.getMessage()) }
      throw dp.getDiagnostics(0).getSomeCause
    }

    //
    // Until https://issues.apache.org/jira/browse/DAFFODIL-1440 is fixed
    // so the Daffodil API objects are serializable, we have to serialize
    // the data processor so that spark can move it around a cluster.
    //
    // Once that issue is fixed, just use the dp directly.
    //
    val baos = new ByteArrayOutputStream()
    dp.save(Channels.newChannel(baos))
    val dpBytes = baos.toByteArray()

    // Standard Spark configuration
    val conf = new SparkConf().setAppName("Daffodil Spark Test").set("spark.master", "local")
    val sc = new SparkContext(conf)

    // Create an RDD (essentially just a collection of items) where each item
    // in this collection is a PortableDataStream (a serializable wrapper
    // around DataInputStream) of a file in the directory specified by args(1).
    // Note that binaryFiles is marked as "experimental". Not sure if that's
    // because there are instabilities, or perhaps just some performance
    // issues
    val dataFiles = sc.binaryFiles(args(1))

    // For each file, map it to a NodeSeq by parsing it with the Daffodil data
    // processor created earlier
    val pcapNodes = dataFiles.map {
      case (filename, data) =>
        //
        // Once again, Until https://issues.apache.org/jira/browse/DAFFODIL-1440 is fixed,
        // we have to deserialize the Daffodil data processor ourselves.
        //
        val dp = getDP(dpBytes)

        val d = data.open()
        val output = new ScalaXMLInfosetOutputter // A JSON outputter is also available!
        val input = new InputSourceDataInputStream(d)
        val pr = dp.parse(input, output)
        val res = output.getResult()
        d.close()
        // log.warn("Parsing Complete")
        res
    }

    //
    // Above here, all the code is generic. Makes sense for any data format.
    //
    // Below here, this code is specific to the PCAP data schema, and is just illustrating
    // convenient XML access using Scala's XML library, within Spark code that is
    // potentially running in parallel/streaming mode.
    //

    // Extract all the Packets from each pcap file, and flat map them all to a
    // single collection
    val packetNodes = pcapNodes.flatMap { node => (node \ "Packet") }

    // Extract all ipv4 packets
    val ipv4Packets = packetNodes.filter { packetNode => (packetNode \ "LinkLayer" \ "Ethernet" \ "NetworkLayer" \ "IPv4").nonEmpty }

    // Map the ipv4 packets to and src/dest key-value pair
    val ipSrcDest = ipv4Packets.map { packetNode => ((packetNode \\ "IPSrc").text, (packetNode \\ "IPDest").text) }

    // Aggregate the src/dest key-value pairs so that the key is the src and
    // the value is the list of all unique IP destinations for that src
    val ipSrcDestMapping = ipSrcDest.aggregateByKey(mutable.HashSet.empty[String])({ case (hs, dest) => hs += dest }, { case (hs1, hs2) => hs1 ++ hs2 })

    // collect the results from the clusters, printing out the src and the set
    // of unique destinations for the source
    ipSrcDestMapping.collect.sortBy { _._1 }.foreach { case (src, dst) => println("%s -> %s".format(src, dst)) }

  }
}
