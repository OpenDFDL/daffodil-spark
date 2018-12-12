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
import java.util.zip.GZIPInputStream
import java.io.InputStream
import org.apache.spark.rdd.RDD
import scala.xml.Elem
import org.apache.daffodil.sapi.infoset.ScalaXMLInfosetOutputter
import org.apache.daffodil.sapi.io.InputSourceDataInputStream

object DaffodilSpark {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("JOB")

  //
  // You could cache this to avoid re-reloading the data processor for every file.
  // However, be careful. Must be distinct for each file.
  // Trivial caching in a member of this object may cause all sorts of problems.
  //
  // parsed. It should no longer be necessary once we get rid of use of the dpBytes
  // I.e., when https://issues.apache.org/jira/browse/DAFFODIL-1440 is fixed.
  //
  def getDP(dpBytes: Array[Byte]) = {
    Daffodil.compiler().reload(Channels.newChannel(new ByteArrayInputStream(dpBytes)))
  }

  def compileToByteArray(schemaArg: String) = {
    //
    // create a Daffodil DataProcessor using schemaArg as the schema. This
    // DataProcessor is used by the mapping functions, and so Spark send
    // it to the clusters, which will then use it in the map steps.

    val c = Daffodil.compiler()
    val pf = c.compileSource(new URI(schemaArg))
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
    dpBytes
  }

  val conf = new SparkConf().setAppName("Daffodil Spark Test").set("spark.master", "local").set("spark.cores.max", "10")
  // Standard Spark configuration
  val sc = new SparkContext(conf)

  /**
   * Splits up data into a scalable collection of XML elements using Daffodil to parse.
   */
  def toRDDElem(dpBytes: Array[Byte], dataFilesArg: String): RDD[Elem] = {

    // Create an RDD (essentially just a collection of items) where each item
    // in this collection is a PortableDataStream (a serializable wrapper
    // around DataInputStream) of a file in the directory specified by dataFilesArg
    // Note that binaryFiles is marked as "experimental". Not sure if that's
    // because there are instabilities, or perhaps just some performance
    // issues
    val dataFiles = sc.binaryFiles(dataFilesArg)

    // For each file, parse it into multiple nodes by parsing repeatedly until the end is reached.
    // Also stops on a parse error.
    //
    // This treats each file as a "stream of messages", each "message" is one parse of the schema.
    // If the file is one giant object, you'll get a sequence of one Elem for each file.
    // But if the file is many smaller objects, it will split them up and you will get a
    // parallelizable spark collection from them.
    //
    val nodes = dataFiles.flatMap {
      case (filename, data) =>
        //
        // Once again, Until https://issues.apache.org/jira/browse/DAFFODIL-1440 is fixed,
        // we have to deserialize the Daffodil data processor ourselves.
        //
        val dp = getDP(dpBytes)
        val isGZ = filename.endsWith("gz")

        val d = data.open()
        val uncompressedStream =
          if (isGZ)
            new GZIPInputStream(d)
          else
            d
        toIteratorElem(uncompressedStream, dp)

    }
    nodes
  }

  def toIteratorElem(d: InputStream, dp: DataProcessor) = {
    val output = new ScalaXMLInfosetOutputter // A JSON outputter is also available!
    //
    // If file name ends in "gz", then automatically decompress it as we parse
    //
    val input = new InputSourceDataInputStream(d)
    //
    // This call the the parse has to be sequential. If each parse is fast
    // then this will scoot through the data quickly, splitting it up for
    // subsequent processing in parallel when the nodes are consumed.
    //
    var done = false
    val nodeIter = Iterator.continually {
      if (done) None
      else {
        val pr = dp.parse(input, output)
        if (pr.isError()) {
          d.close()
          done = true
          //
          // TODO: Do the right thing in Spark to indicate an error in the data.
          // We may not want to stop processing entirely, but we don't want to mask the
          // error by silently just truncating the data to the part we parsed
          // successfully.
          val diags = pr.getDiagnostics
          val th = diags(0).getSomeCause //  there has to be at least 1 if we are in error state.
          throw th
          None
        } else {
          if (pr.location().isAtEnd()) {
            d.close()
            done = true
          }
          val res = output.getResult()
          Some(res.asInstanceOf[scala.xml.Elem])
        }
      }
    }
    val nodes = nodeIter.takeWhile(_.isDefined).flatten
    nodes
  }

}
