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

import java.nio.file.Files
import java.util.zip.GZIPInputStream

import scala.collection.mutable

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.junit.Assert._
import org.junit.Test
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.hadoop.io.compress.GzipCodec

class UnitTestDaffodilSparkPCAP {

  @Test
  def testPCAP1(): Unit = {
    val sch = getClass().getResource("/com/tresys/pcap/xsd/pcap.dfdl.xsd").toString
    val dataFile = getClass().getResource("/com/tresys/pcap/data/dns.cap").toString

    val dpBytes = DaffodilSpark.compileToByteArray(sch)
    val nodes = DaffodilSpark.toRDDElem(dpBytes, dataFile)

    // Extract all the Packets from each pcap file, and flat map them all to a
    // single collection
    val packetNodes = nodes.flatMap { node => (node \ "Packet") }

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
