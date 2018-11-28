# daffodil-spark

An example of using Apache Daffodil with Apache Spark.

This example shows how easy it is to use Daffodil to parse
data that is described by a DFDL schema, and feed that data into 
Spark processing for parallel/scalable treatment.

The example and unit test use PCAP (Packet capture) which is a 
binary data format that is useful as an example because binary data is
exactly the kind of data people find hardest to access.

The techniques here can be adapted to any data format described by
a DFDL schema.

The copy of the PCAP schema included here for testing/example may not be 
the latest one. Look on github for a more current one if you are 
actually interested in PCAP data per se.

== Additional thoughts for spark enthusiasts ==

* JSON - there's a Daffodil "Outputter" for JSON also. You can easily
modify what is here to construct a JSON string from the data if you prefer
or need that representation.

* Spark Struct - It appears that Daffodil could be directly interfaced to 
Spark, converting a DFDL schema to a Spark Struct object, and populating a 
struct directly. DFDL schemas are a subset of XML schemas that very precisely 
match the kind of things describable directly in a Spark Struct. DFDL leaves out
the "markup language" aspects of XML Schema, and keeps the "data format language"
aspects, which makes it align very tightly with Spark Structs because both were
intended to describe "structured data". This integration would provide seamless
metadata and data bridging and would be more efficient than converting first to XML or JSON. 
** Mentioning this, hoping someone will be enthusiastic enough about both Spark
and Daffodil to do it! Please contact users@daffodil.apache.org or 
dev@daffodil.apache.org if interested.