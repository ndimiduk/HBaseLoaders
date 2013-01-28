package com.hortonworks.examples.hbase.wikitraffic;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hortonworks.examples.hbase.wikitraffic.hbase.WikistatsSchemaUtils;

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
      System.err.printf("Usage: %s [generic options] (%s | %s) <target-table> <input-path>%n",
        getClass().getSimpleName(), TALL, WIDE);
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    String schemaType  = args[0];
    String targetTable = args[1];
    String inputPath   = args[2];

    // validate destination table
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(getConf());
      // verify table and column exist
      if (admin.tableExists(targetTable)) {
        HColumnDescriptor[] cols =
            admin.getTableDescriptor(Bytes.toBytes(targetTable)).getColumnFamilies();
        boolean hasTargetColumn = false;
        for (HColumnDescriptor desc : cols) {
          if (Arrays.equals(WikistatsSchemaUtils.COLUMN_FAMILY_NAME, desc.getName()))
            hasTargetColumn = true;
        }
        if (hasTargetColumn) {
          LOG.info(String.format("Using existing table '%s'", targetTable));
        } else {
          LOG.error(String.format("Target table '%s' does not have required column family '%s'.",
            targetTable, Bytes.toString(WikistatsSchemaUtils.COLUMN_FAMILY_NAME)));
          return -1;
        }
      } else {
        LOG.info(String.format("Table '%s' does not exist. Creating from default descriptor.",
            targetTable));
        admin.createTable(WikistatsSchemaUtils.createDefaultTableDesc(targetTable));
      }
    } catch (IOException e) {
      LOG.error("Failed to verify target table. Aborting.", e);
      return -1;
    } finally {
      if (null != admin)
        admin.close();
    }

    // initialize job
    Job job  = new Job(getConf(), "Populate Wikistats table.");
    job.setJarByClass(getClass());

    // configure job input path
    Path wikistats = new Path(inputPath);
    FileInputFormat.addInputPath(job, wikistats);

    // configure job mapper
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(schemaType.equals(TALL)
        ? WikistatsSchemaUtils.TallWikistatsMapper.class
        : WikistatsSchemaUtils.WideWikistatsMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
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
