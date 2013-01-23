# Wikistats Example Loaders

The project contains example [Hadoop MapReduce][0] applications for
efficiently loading data into [Apache HBase][1]. The applications are
designed to operate against the [Wikipedia Page Traffic Statistics][2]
dataset, a public dataset available on AWS.

`WikistatsOnlineLoader` uses the online API, issuing Put requests
against the running cluster. `WikistatsHFileLoader` generates HFiles
directly from MapReduce. It does so in a way that is entirely
decoupled from a running HBase cluster.

## Usage

The project is managed by Maven, just like HBase. Create the
application jar using:

    $ mvn clean package

This produces the assembled application jar in the `target` directory.

### Online Loader

Assuming you have `hbase` installed in your `PATH` and configured to
point to your target cluster, run the application like this:

    $ HADOOP_CLASSPATH=`hbase classpath` hadoop jar \
      target/HBaseLoaders-0.1.0-SNAPSHOT.jar \
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

    $ HADOOP_CLASSPATH=`hbase classpath` hadoop jar \
      target/HBaseLoaders-0.1.0-SNAPSHOT.jar \
      com.hortonworks.examples.hbase.wikitraffic.WikistatsOnlineLoader \
      -Dlog4j.configuration=file:./src/main/resources/log4j.properties \
      -tall wikistats ./sampledata/pagecounts-20090430-230000.txt

### HFile Loader

> This loader is incomplete in that it only generates HFiles. It does
> not perform the final step of executing `completebulkload`.

Assuming you have `hbase` installed in your `PATH` and configured to
point to your target cluster, run the application like this:

    $ HADOOP_CLASSPATH=`hbase classpath` hadoop jar \
      target/HBaseLoaders-0.1.0-SNAPSHOT.jar \
      com.hortonworks.examples.hbase.wikitraffic.WikistatsHFileLoader \

> There is a known issue when running a hadoop in local mode. You'll
> likely see class not found errors relating to the ReservoirSampler.
> The simplest solution is to manually include that jar in your
> classpath, like so:

    $ HBASE_CLASSPATH=...:$HOME/.m2/repository/org/clojars/ndimiduk/reservoirsampler/0.1.0/reservoirsampler-0.1.0.jar

Providing the loader with no arguments will print a short help
message. For operation, the loader accepts three positional arguments:

    -tall | -wide  Specify to use either a "tall" or "wide" schema for
                   loading the data into HBase.
    <target-table> The table into which data is loaded. The loader
                   will attempt to create the table using the default
                   descriptor if it does not exist.
    <num-splits>   The number of splits to create. These are roughly
                   equivalent to region splits in the final table.
    <input-path>   A full path to the wikistats pagecount directory.
                   Alternately, the path to a single file can be
                   provided to load a small subset of the data.
    <working-path> A working directory for this invocation of the
                   Loader, a place where it can create a number of
                   intermediary files.

A sample of the data is provided in the repository. Out of the box, an
invocation might look like this:

    $ HADOOP_CLASSPATH=`hbase classpath` hadoop jar \
      target/HBaseLoaders-0.1.0-SNAPSHOT.jar \
      com.hortonworks.examples.hbase.wikitraffic.WikistatsHFileLoader \
      -Dlog4j.configuration=file:./src/main/resources/log4j.properties \
      -tall wikistats 1 ./sampledata/pagecounts-20090430-230000.txt work

## License

Copyright © 2013 Hortonworks, Inc.

Distributed under [The Apache Software License, Version 2.0][3].

[0]: http://hadoop.apache.org/
[1]: http://hbase.apache.org/
[2]: http://aws.amazon.com/datasets/2596
[3]: http://www.apache.org/licenses/LICENSE-2.0
