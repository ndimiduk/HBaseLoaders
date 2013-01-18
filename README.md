# Wikistats Online Loader

WikistatsOnlineLoader is a [Hadoop MapReduce][0] application that
demonstrates how to efficiently load data into [Apache HBase][1]. It
uses the online API, issuing Put requests against the running cluster.
The application is designed to operate against the
[Wikipedia Page Traffic Statistics][2] dataset, a public dataset
available on AWS.

## Usage

The project is managed by Maven, just like HBase. Create the
application jar using:

    $ mvn clean package

This produces the assembled application jar in the `target` directory.
Assuming you have `hbase` installed in your `PATH` and configured to
point to your target cluster, run the application like this:

    $ HBASE_CLASSPATH=`pwd`/target/WikistatsOnline-*.jar hbase \
      com.hortonworks.examples.hbase.wikitraffic.WikistatsOnlineLoader

Providing the loader with no arguments will print a short help
message. For operation, the loader accepts three positional arguments:

    -tall | -wide  Specify to use either a "tall" or "wide" schema for
                   loading the data into HBase.
    <target-table> The table into which data is loaded. The loader
                   will attempt to create the table using the default
                   descriptor if it does not exist.
    <input-path>   A full path to the wikistats pagecount directory.
                   Alternately, the path to a single file can be
                   provided to load a small subset of the data.

A sample of the data is provided in the repository. Out of the box, an
invocation might look like this:

    $ HBASE_CLASSPATH=`pwd`/target/WikistatsOnline-*.jar hbase \
      -Dlog4j.configuration=file:./src/main/resources/log4j.properties \
      com.hortonworks.examples.hbase.wikitraffic.WikistatsOnlineLoader \
      -tall wikistats ./sampledata/pagecounts-20090430-230000.txt

## License

Copyright © 2013 Hortonworks, Inc.

Distributed under [The Apache Software License, Version 2.0][3].

[0]: http://hadoop.apache.org/
[1]: http://hbase.apache.org/
[2]: http://aws.amazon.com/datasets/2596
[3]: http://www.apache.org/licenses/LICENSE-2.0
