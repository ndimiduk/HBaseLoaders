package com.hortonworks.examples.hbase.wikitraffic;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.hortonworks.examples.hbase.wikitraffic.hbase.WikistatsSchemaUtils;
import com.hortonworks.examples.hbase.wikitraffic.mapreduce.NullValueMapper;
import com.hortonworks.examples.hbase.wikitraffic.mapreduce.io.FilePathTextInputFormat;
import com.manning.hip.ch4.sampler.ReservoirSamplerInputFormat;

public class WikistatsHFileLoader extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(WikistatsHFileLoader.class);
  private static final String TALL = "-tall";
  private static final String WIDE = "-wide";

  private Job buildPrepareDataJob(String schemaType, Path inputPath,
      Path outputPath) throws IOException {
    Job job = new Job(getConf(), "Prepare raw data.");
    job.setJarByClass(getClass());

    job.setInputFormatClass(FilePathTextInputFormat.class);
    FilePathTextInputFormat.addInputPath(job, inputPath);

    job.setMapperClass(schemaType.equals(TALL)
      ? WikistatsSchemaUtils.TallWikistatsMapper.class
      : WikistatsSchemaUtils.WideWikistatsMapper.class);
    job.setNumReduceTasks(0);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, outputPath);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Put.class);

    TableMapReduceUtil.addDependencyJars(job);
    return job;
  }

  private Job buildSampleJob(int numSplits, Path inputPath, Path outputPath)
      throws IOException {
    Job job = new Job(getConf(), "Sample input dataset.");
    job.setJarByClass(getClass());

    SequenceFileInputFormat.setInputPaths(job, inputPath);
    ReservoirSamplerInputFormat.setInputFormat(job, SequenceFileInputFormat.class);
    ReservoirSamplerInputFormat.setNumSamples(job, numSplits);
    ReservoirSamplerInputFormat.setMaxRecordsToRead(job, 10000);
    ReservoirSamplerInputFormat.setUseSamplesNumberPerInputSplit(job, false);

    job.setMapperClass(NullValueMapper.class);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, outputPath);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(NullWritable.class);

    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
      ReservoirSamplerInputFormat.class, Put.class);
    return job;
  }

  private Job buildHFilesJob(String tableName, int numSplits, Path splitsFile, Path inputPath,
      Path outputPath) throws IOException {
    Job job = new Job(getConf(), "Generate HFiles.");
    job.setJarByClass(getClass());

    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, inputPath);

    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);

    job.setPartitionerClass(TotalOrderPartitioner.class);
    TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), splitsFile);

    job.setReducerClass(PutSortReducer.class);
    job.setNumReduceTasks(numSplits + 1);

    job.setOutputFormatClass(HFileOutputFormat.class);
    HFileOutputFormat.setOutputPath(job, outputPath);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(KeyValue.class);

    TableMapReduceUtil.addDependencyJars(job);
    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
      com.google.common.base.Preconditions.class);
    return job;
  }

  private Path getSplitsFile(FileSystem fs, Path splitsOutput) throws IOException {
    FileStatus[] files = fs.listStatus(splitsOutput, new OutputFilesFilter());
    assert files.length == 1 : "Splits job created too many output files.";
    return files[0].getPath();
  }

  @Override
  public int run(String[] args) throws Exception {
    boolean argsValid = true;
    argsValid = argsValid && args.length == 5;
    argsValid = argsValid && (TALL.equals(args[0]) || WIDE.equals(args[0]));

    if (!argsValid) {
      System.err.printf("Usage: %s [generic options] (%s | %s) <target-table> <num-splits> <input-path> <working-path>%n",
        getClass().getSimpleName(), TALL, WIDE);
      System.err.println();
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }

    String schemaType  = args[0];
    String targetTable = args[1];
    int numSplits      = Integer.parseInt(args[2]);
    Path wikistats     = new Path(args[3]);
    Path workingPath   = new Path(args[4]);

    Path preparedData  = new Path(workingPath, "prepared");
    Path splits        = new Path(workingPath, "splits");
    Path hfiles        = new Path(workingPath, "hfiles");

    // create/clean workspace
    FileSystem fs = FileSystem.get(getConf());
    fs.mkdirs(workingPath);
    for (Path p : new Path[] {preparedData, splits, hfiles}) {
      fs.delete(p, true);
    }

    Job prepareJob = buildPrepareDataJob(schemaType, wikistats, preparedData);
    LOG.info("Preparing input data for HBase schema.");
    if (!prepareJob.waitForCompletion(true))
      return -1;

    Job sampleJob = buildSampleJob(numSplits, preparedData, splits);
    LOG.info("Sampling input data to determine region splits.");
    if (!sampleJob.waitForCompletion(true))
      return -1;

    Path splitsFile = getSplitsFile(fs, splits);

    Job hfilesJob = buildHFilesJob(targetTable, numSplits, splitsFile, preparedData, hfiles);
    LOG.info("Generating HFiles from prepared data according to splits.");
    if (!hfilesJob.waitForCompletion(true))
      return -1;

    return 0;
  }

  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new WikistatsHFileLoader(), args);
    System.exit(status);
  }
}
