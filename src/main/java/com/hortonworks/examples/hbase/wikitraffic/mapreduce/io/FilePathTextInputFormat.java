package com.hortonworks.examples.hbase.wikitraffic.mapreduce.io;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * Acts just like {@link TextInputFormat}, except the key is a {@link Text}
 * instance of the form {@code "filePath:position"}.
 */
public class FilePathTextInputFormat extends FileInputFormat<Text, Text> {

  @Override
  public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
  throws IOException, InterruptedException {
    return new PathLineRecordReader();
  }

  /*
   * copied from TextInputFormat. Cannot use inheritance because of difference
   * in generic signature.
   */
  @Override
  protected boolean isSplitable(JobContext context, Path file) {
    final CompressionCodec codec =
      new CompressionCodecFactory(context.getConfiguration()).getCodec(file);
    if (null == codec) {
      return true;
    }
    return codec instanceof SplittableCompressionCodec;
  }

  /**
   * Implicitly wraps a {@link LineRecordReader}, producing records keyed on
   * {@code "filePath:position"} instead of {@code position}. Most of the
   * implementation delegates to {@link LineRecordReader}.
   * 
   * {@code position} is formatted in the Text instance as a decimal integer. 
   */
  public static class PathLineRecordReader extends RecordReader<Text, Text> {

    private LineRecordReader lineReader;
    private String path;

    public PathLineRecordReader() {
      lineReader = new LineRecordReader();
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
      lineReader.initialize(genericSplit, context);
      FileSplit split = (FileSplit) genericSplit;
      this.path = split.getPath().toString();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      return lineReader.nextKeyValue();
    }
    
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
      LongWritable position = lineReader.getCurrentKey();
      return new Text(String.format("%s:%d", path, position.get()));
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return lineReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
      lineReader.close();
    }
  }
}
