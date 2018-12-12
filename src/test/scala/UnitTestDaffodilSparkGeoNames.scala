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

class UnitTestDaffodilSparkGeoNames {

  /**
   * Exercise the daffodil-specific machinery for parsing geonames data.
   * Doesn't use Spark at all.
   */
  @Test
  def testGeoNamesNonSpark(): Unit = {
    val sch = getClass().getResource("/com/tresys/geonames/xsd/geonames.dfdl.xsd").toString
    val dataFile = getClass().getResource("/com/tresys/geonames/geonames-rdf-snip.txt.gz")

    val dpBytes = DaffodilSpark.compileToByteArray(sch)

    val in = new GZIPInputStream(dataFile.openStream())
    // val in = dataFile.openStream()
    val dp = DaffodilSpark.getDP(dpBytes)
    val nodes = DaffodilSpark.toIteratorElem(in, dp)
    val strings =
      nodes.map { n =>
        val rdfLines = (n \\ "RDFLine").text
        """<geoname>
  <rdf:RDF>
  """ + rdfLines + """
  </rdf:RDF>
</geoname>
"""
      }.toList.mkString
    val wholeXMLString = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<geo:geonames
xmlns:geo="http://www.geonames.org/xml"
xmlns:cc="http://creativecommons.org/ns#"
xmlns:dcterms="http://purl.org/dc/terms/"
xmlns:foaf="http://xmlns.com/foaf/0.1/"
xmlns:gn="http://www.geonames.org/ontology#"
xmlns:owl="http://www.w3.org/2002/07/owl#"
xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
xmlns:wgs84_pos="http://www.w3.org/2003/01/geo/wgs84_pos#">
""" + strings + """</geo:geonames>
"""
    val actualXML = scala.xml.XML.loadString(wholeXMLString)
    val pp = new scala.xml.PrettyPrinter(80, 2)
    println(pp.format(actualXML))

    val expectedFile = getClass().getResource("/com/tresys/geonames/geonames-rdf-snip.xml")
    val expectedXML = scala.xml.XML.load(expectedFile)

    //
    // We must trim in order to compare, in order to remove insignificant whitespace
    // from the comparison
    //
    assertEquals(scala.xml.Utility.trim(actualXML), scala.xml.Utility.trim(expectedXML))

  }

  /**
   * This test shows how to create a spark + daffodil application that breaks up the giant geonames
   * data file into a bunch of smaller files, and converts it from quasi-XML into actual XML
   * files.
   */
  @Test
  def testGeoNamesSpark(): Unit = {
    val sch = getClass().getResource("/com/tresys/geonames/xsd/geonames.dfdl.xsd").toString
    val dataFile = getClass().getResource("/com/tresys/geonames/geonames-rdf-set1.txt.gz")

    val dpBytes = DaffodilSpark.compileToByteArray(sch)

    val nodes = DaffodilSpark.toRDDElem(dpBytes, dataFile.toString)

    val strings =
      nodes.map { n =>
        val rdfLines = (n \\ "RDFLine").text
        """<geoname>
  <rdf:RDF>
  """ + rdfLines + """
  </rdf:RDF>
</geoname>
"""
      }.repartition(16) // Let's get some partitions going (even for just small test data, just to see it working
    // Note: Take this out if you have a "real" spark cluster.

    val numFiles = 8 // how many files??

    // Ideally we'd want to partition the data by a useful key (like parent feature)
    // Also we'd like to have the file sizes limited by say, N records per file, but
    // respecting the grouping keys, rather than deciding the number of files up front.

    val stringGroups = strings.coalesce(numFiles, false, None)
    val partitioned = stringGroups.mapPartitions { p =>
      val strings = p.toList.mkString
      val wholeXMLString = """<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<geo:geonames
xmlns:geo="http://www.geonames.org/xml"
xmlns:cc="http://creativecommons.org/ns#"
xmlns:dcterms="http://purl.org/dc/terms/"
xmlns:foaf="http://xmlns.com/foaf/0.1/"
xmlns:gn="http://www.geonames.org/ontology#"
xmlns:owl="http://www.w3.org/2002/07/owl#"
xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
xmlns:wgs84_pos="http://www.w3.org/2003/01/geo/wgs84_pos#">
""" + strings + """</geo:geonames>
"""
      val actualXML = scala.xml.XML.loadString(wholeXMLString)

      // At this point we have an actual piece of scala XML which contains a bunch of
      // these geoname records.
      //
      // We want it to be readable in files, so pretty print to string.
      val pp = new scala.xml.PrettyPrinter(80, 2)
      val prettyString = pp.format(actualXML)

      // We return an iterator, since that's what mapPartitions requires.
      Some(prettyString).toIterator
    }

    val tmpDir = new File("/tmp/geonames/")
    FileUtils.deleteDirectory(tmpDir)

    //
    // These are big XML files, so let's gzip them.
    //
    // You can still look at an individual partition easily enough with zcat. The partition files
    // themselves nicely have a ".gz" extention on them.

    partitioned.saveAsTextFile(tmpDir.toString, classOf[GzipCodec])
  }

}
