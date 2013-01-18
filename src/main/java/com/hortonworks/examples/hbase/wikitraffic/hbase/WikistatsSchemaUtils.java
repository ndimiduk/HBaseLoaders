package com.hortonworks.examples.hbase.wikitraffic.hbase;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;

import com.hortonworks.examples.hbase.wikitraffic.mapreduce.WikistatsMapper;

public class WikistatsSchemaUtils {

  // column family settings
  public static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("p");
  public static final int MAX_VERSIONS = 1;
  public static final Algorithm DEFAULT_COMPRESSION = Algorithm.NONE;

  // qualifier settings
  public static final byte[] VIEWS_QUAL = Bytes.toBytes("views");
  public static final byte[] BYTES_QUAL = Bytes.toBytes("bytes");

  /**
   * Create a table descriptor from default values.
   * @return the {@link HTableDescriptor}.
   */
  public static HTableDescriptor createDefaultTableDesc(String tableName) {
    HColumnDescriptor family = new HColumnDescriptor(COLUMN_FAMILY_NAME)
      .setCompressionType(DEFAULT_COMPRESSION)
      .setMaxVersions(MAX_VERSIONS);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(family);
    return desc;
  }

  /**
   * Implements the "tall schema" for the wikistats dataset. Each record is
   * stored as <tt>projectCode/pageName/dateTime => {p:views, p:bytes}</tt>.
   * All serialization is handled via {@link Bytes}.
   * 
   * @param dateTime Date/Time of the hour of this observation.
   * @param projectCode Wikipedia project code of the page viewed.
   * @param pageName Wikipedia name of the page viewed.
   * @param pageViews Number of page views for this hour.
   * @param bytes Size of the resource requested in bytes.
   * @return A {@link Put} instance representing the record.
   */
  public static Put createTallPut(String dateTime, String projectCode,
      String pageName, long pageViews, long bytes) {
    
    String rowkey = String.format("%s/%s/%s", projectCode, pageName, dateTime);
    Put put = new Put(Bytes.toBytes(rowkey));
    put.add(COLUMN_FAMILY_NAME, VIEWS_QUAL, Bytes.toBytes(pageViews));
    put.add(COLUMN_FAMILY_NAME, BYTES_QUAL, Bytes.toBytes(bytes));

    return put;
  }

  /**
   * Implements the "wide schema" for the wikistats dataset. Each record is
   * stored as <tt>projectCode/pageName => {p:dateTime =>
   * json-serialized({views, bytes})}</tt>. All serialization is handled via
   * {@link Bytes}.
   * 
   * @param dateTime Date/Time of the hour of this observation.
   * @param projectCode Wikipedia project code of the page viewed.
   * @param pageName Wikipedia name of the page viewed.
   * @param pageViews Number of page views for this hour.
   * @param bytes Size of the resource requested in bytes.
   * @return A {@link Put} instance representing the record.
   */
  public static Put createWidePut(String dateTime, String projectCode,
      String pageName, long pageViews, long bytes) {
    
    String rowkey = String.format("%s/%s", projectCode, pageName);
    // poor-man's JSON serialization.
    String value = String.format("{\"views\":\"%s\",\"bytes\":%d", pageViews, bytes);
    Put put = new Put(Bytes.toBytes(rowkey));
    put.add(COLUMN_FAMILY_NAME, Bytes.toBytes(dateTime), Bytes.toBytes(value));

    return put;
  }

  /**
   * Implements the "tall schema" for the wikistats dataset. Each record is
   * stored as <tt>projectCode/pageName/dateTime => {p:views, p:bytes}</tt>.
   * All serialization is handled via {@link Bytes}.
   */
  public static class TallWikistatsMapper extends WikistatsMapper {

    @Override
    public Put createPut(String dateTime, String projectCode, String pageName,
        long pageViews, long bytes) {
      return createTallPut(dateTime, projectCode, pageName, pageViews, bytes);
    }    
  }

  /**
   * Implements the "wide schema" for the wikistats dataset. Each record is
   * stored as <tt>projectCode/pageName => {p:dateTime =>
   * json-serialized({views, bytes})}</tt>. All serialization is handled via
   * {@link Bytes}.
   */
  public static class WideWikistatsMapper extends WikistatsMapper {

    @Override
    public Put createPut(String dateTime, String projectCode, String pageName,
        long pageViews, long bytes) {
      return createWidePut(dateTime, projectCode, pageName, pageViews, bytes);
    }
  }
}
