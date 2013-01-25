package com.hortonworks.examples.hbase.wikitraffic.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.hortonworks.examples.hbase.wikitraffic.WikistatsOnlineLoader;
import com.hortonworks.examples.hbase.wikitraffic.mapreduce.io.FilePathTextInputFormat;

/**
 * Reads text lines from Wikistats and produces HBase {@code rowkey => Put}
 * pairs. Wikistats data comes in text lines as
 * {@code projectcode, pagename, pageviews, bytes}. Datetime is parsed from
 * the filename, provided in the key via {@link FilePathTextInputFormat}.
 */
public abstract class WikistatsMapper extends Mapper<Text, Text, ImmutableBytesWritable, Put> {

  private static final Log LOG = LogFactory.getLog(WikistatsOnlineLoader.class);
  private static final Pattern keyParser = Pattern.compile("^(.*):(\\d+)$");
  // don't assume a file extension
  private static final Pattern fileNameParser =
      Pattern.compile("^pagecounts-(\\d{8}-\\d{6})\\..*$");

  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Parse the date-time component from the key.
     * 
     * Using the descriptions in `man date` as a reference, each file in the
     * wikistats dataset is of the format "pagecounts-YYYYmmdd-HHMMss.gz". No
     * timezone information is provided in the documentation; assume GMT, so
     * no TZ manipulation will be performed.
     */
    String dt = null;
    long position = -1;

    Matcher m = keyParser.matcher(key.toString());
    if (!m.matches()) {
      LOG.warn(String.format("Failed to parse input key: %s", key.toString()));
      return;
    }

    String fileName = new Path(m.group(1)).getName();
    position = Long.parseLong(m.group(2));
    m = fileNameParser.matcher(fileName);
    if (!m.matches()) {
      LOG.warn(String.format("Failed to parse filename: %s", fileName));
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
        fileName, position, value.toString()));
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
