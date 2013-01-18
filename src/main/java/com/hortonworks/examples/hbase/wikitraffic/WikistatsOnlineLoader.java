package com.hortonworks.examples.hbase.wikitraffic;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hortonworks.examples.hbase.wikitraffic.hbase.WikistatsSchemaUtils;
import com.hortonworks.examples.hbase.wikitraffic.mapreduce.io.FilePathTextInputFormat;

/**
 * An example HBase application. Load the wikitraffic's wikistats dataset
 * into HBase using the online API, populating a 'wide' schema. This example
 * uses raw MapReduce.
 */
public class WikistatsOnlineLoader extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(WikistatsOnlineLoader.class);
  private static final String TALL = "-tall";
  private static final String WIDE = "-wide";

  public int run(String[] args) throws Exception {
    boolean argsValid = true;
    argsValid = argsValid && args.length == 3;
    argsValid = argsValid && (TALL.equals(args[0]) || WIDE.equals(args[0]));

    if (!argsValid) {
      System.err.printf("Usage: %s [generic options] (-tall | -wide) <target-table> <input-path>",
        getClass().getSimpleName());
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    String schemaType = args[0];
    String targetTable = args[1];
    String inputPath = args[2];

    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(getConf());
      if (admin.tableExists(targetTable)) {
        LOG.info(String.format("Using existing table '%s'", targetTable));
      } else {
        LOG.info(String.format("Table '%s' does not exist. Creating from default descriptor.",
            targetTable));
        admin.createTable(WikistatsSchemaUtils.createDefaultTableDesc(targetTable));
      }
    } catch (IOException e) {
      LOG.error("Failed to create table. Aborting.", e);
      return -1;
    } finally {
      if (null != admin)
        admin.close();
    }

    // initialize job
    Job job  = new Job(getConf(), "Populate Wikistats Online, Wide schema.");
    job.setJarByClass(getClass());

    // configure job input path
    Path wikistats = new Path(inputPath);
    FileInputFormat.addInputPath(job, wikistats);

    // configure job mapper
    job.setInputFormatClass(FilePathTextInputFormat.class);
    job.setMapperClass(schemaType.equals(TALL)
        ? WikistatsSchemaUtils.TallWikistatsMapper.class
        : WikistatsSchemaUtils.WideWikistatsMapper.class);
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    // configure job reducer
    TableMapReduceUtil.initTableReducerJob(targetTable, IdentityTableReducer.class, job);

    // include additional HBase jars
    TableMapReduceUtil.addDependencyJars(job);

    // run, time the job.
    long startTime = System.currentTimeMillis();
    boolean success = job.waitForCompletion(true);
    long endTime = System.currentTimeMillis();
    Counter c = job.getCounters().findCounter(
      "org.apache.hadoop.mapred.Task$Counter", "REDUCE_OUTPUT_RECORDS");
    long recordsWritten = c.getValue();
    LOG.info(String.format("Wrote %d %s records to HBase in %d ms",
      recordsWritten, schemaType.equals(TALL) ? "tall" : "wide", (endTime - startTime)));
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new WikistatsOnlineLoader(), args);
    System.exit(status);
  }
}
