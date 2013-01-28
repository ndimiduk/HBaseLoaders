package com.hortonworks.examples.hbase.wikitraffic.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.hortonworks.examples.hbase.wikitraffic.WikistatsOnlineLoader;

/**
 * Reads text lines from Wikistats and produces HBase {@code rowkey => Put}
 * pairs. Wikistats data comes in text lines as
 * {@code projectcode, pagename, pageviews, bytes}. Datetime is parsed from
 * the filename.
 */
public abstract class WikistatsMapper
extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

  private static final Log LOG = LogFactory.getLog(WikistatsOnlineLoader.class);
  // don't assume a file extension
  private static final Pattern fileNameParser =
      Pattern.compile("^.*/pagecounts-(\\d{8}-\\d{6})\\..*$");
  private static final String MAP_INPUT_FILE = "map.input.file";

  private String inputFile = null;

  @Override
  protected void setup(Context context) {

    // TODO: why doesn't configuration work for integration tests? bug?
    this.inputFile = context.getConfiguration().get(MAP_INPUT_FILE, null);
    if (null == inputFile) {
      LOG.warn(
        String.format("%s not set. Interrogating split file path manually.", MAP_INPUT_FILE));
      InputSplit split = context.getInputSplit();
      if (split instanceof FileSplit) {
        this.inputFile = ((FileSplit) split).getPath().toString();
      } else {
        LOG.warn("Unable to determine input file path for this split."
            + " Mapper will produce no records.");
      }
    }
  }

  // Just in case this Mapper instance is reused, avoid annoying bugs.
  @Override
  protected void cleanup(Context context) {
    this.inputFile = null;
  }

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Parse the date-time component from the key.
     * 
     * Using the descriptions in `man date` as a reference, each file in the
     * wikistats dataset is of the format "pagecounts-YYYYmmdd-HHMMss.gz". No
     * timezone information is provided in the documentation; assume GMT, so
     * no TZ manipulation will be performed.
     */
    if (null == this.inputFile) {
      LOG.error("Map context does not provide 'map.input.file'. Aborting.");
      return;
    }

    String dt = null;
    Matcher m = fileNameParser.matcher(this.inputFile);
    if (!m.matches()) {
      LOG.warn(String.format("Failed to parse filename: %s", this.inputFile));
      return;
    }
    dt = m.group(1);

    /*
     * Parse the remaining components from the value.
     * 
     * Each record in the wikistats dataset is a string delimited with Space.
     * Record order is projectcode, pagename, pageviews, bytes.
     */
    String projectCode = null, pageName = null;
    long pageViews = -1, bytes = -1;

    try {
      String splits[] = value.toString().split(" ", 4);
      projectCode = splits[0];
      pageName = splits[1];
      pageViews = Long.parseLong(splits[2]);
      bytes = Long.parseLong(splits[3]);
    } catch (IllegalArgumentException e) {
      LOG.warn(String.format("Failed to parse record in file %s, position %d: %s",
        this.inputFile, key.get(), value.toString()));
      return;
    }

    Put put = createPut(dt, projectCode, pageName, pageViews, bytes);
    context.write(new ImmutableBytesWritable(put.getRow()), put);
  }

  /**
   * Create a {@link Put} instance from a wikistats record.
   * @param dateTime Date/Time of the hour of this observation.
   * @param projectCode Wikipedia project code of the page viewed.
   * @param pageName Wikipedia name of the page viewed.
   * @param pageViews Number of page views for this hour.
   * @param bytes Size of the resource requested in bytes.
   * @return A {@link Put} instance representing the record.
   */
  public abstract Put createPut(String dateTime, String projectCode, String pageName,
      long pageViews, long bytes);
}
