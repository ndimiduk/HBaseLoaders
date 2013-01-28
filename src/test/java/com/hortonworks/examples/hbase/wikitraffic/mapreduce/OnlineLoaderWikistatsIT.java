package com.hortonworks.examples.hbase.wikitraffic.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.LineReader;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.hortonworks.examples.hbase.wikitraffic.WikistatsOnlineLoader;

@RunWith(JUnit4.class)
public class OnlineLoaderWikistatsIT {

  private static final String TABLE_NAME = "OnlineIntegrationTest";
  private static final String INPUT_PATH = "./sampledata";
  private static Configuration conf = HBaseConfiguration.create();

  private HBaseAdmin admin;

  /**
   * Count lines read from <tt>in</tt>. Consumes the underlying stream entirely
   * but does not close it.
   * @param in an {@link InputStream} to consume.
   * @param codec a {@link CompressionCodec} used when reading in. Pass null
   *              when in is not compressed.
   * @return the number of lines read.
   */
  private static long countLines(InputStream in, CompressionCodec codec) throws IOException {
    long cnt = 0;
    Text str = new Text();
    LineReader reader;
    if (codec == null) {
      reader = new LineReader(in);
    } else {
      reader = new LineReader(codec.createInputStream(in));
    }
    while (0 != reader.readLine(str)) {
      cnt++;
    }
    return cnt;
  }

  /**
   * Count all the records in the provided <tt>path</tt>. In the event path is
   * a directory, counts all contained files but does not recurse.
   * @param fs a {@link FileSystem} instance to use while interacting with
   *           <tt>path</tt>.
   * @param path {@link Path} to a file or directory.
   * @return the number of lines in the contained files.
   */
  private static long countRows(FileSystem fs, Path path) throws IOException {
    long cnt = 0;
    FileStatus[] srcs = fs.listStatus(path);
    if (null == srcs || srcs.length == 0) {
      return 0;
    }

    for(FileStatus src : srcs) {
      if (src.isDir()) {
        continue;
      }
      CompressionCodecFactory codecs = new CompressionCodecFactory(conf);
      CompressionCodec codec = codecs.getCodec(src.getPath());
      InputStream fin = fs.open(src.getPath());
      cnt += countLines(fin, codec);
      fin.close();
    }
    return cnt;
  }

  /**
   * Count all rows in <tt>table</tt>.
   * @param table the {@link HTable} whose content is counted.
   * @return the number of rows in <tt>table</tt>.
   */
  private static long countRows(HTable table) throws IOException {
    long cnt = 0;
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setCaching(1000);
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner results = table.getScanner(scan);
    for (Iterator<Result> it = results.iterator(); it.hasNext(); it.next())
      cnt++;
    results.close();

    return cnt;
  }

  @BeforeClass
  public static void populateHDFS() throws IOException {
    // copy sample data to test cluster's hdfs
    Path src = new Path(INPUT_PATH);
    Path dst = new Path("./");
    FileSystem dstFs = dst.getFileSystem(conf);
    dstFs.copyFromLocalFile(false, false, src, dst);
  }

  @Before
  public void cleanTheSlate() throws IOException {
    // nuke the table if it exists.
    this.admin = new HBaseAdmin(conf);
    if (admin.tableExists(TABLE_NAME)) {
      admin.disableTable(TABLE_NAME);
      admin.deleteTable(TABLE_NAME);
    }
    this.admin.close();
  }

  @Test
  public void positiveWideTest() throws Exception {
    String args[] = { "-wide", TABLE_NAME, INPUT_PATH };
    WikistatsOnlineLoader loader = new WikistatsOnlineLoader();
    loader.setConf(conf);
    int ret = loader.run(args);

    // verify job existed cleanly
    assertEquals(0, ret);

    // verify the number of records written, by counting
    Path inputPath = new Path(INPUT_PATH);
    FileSystem fs = FileSystem.get(conf);
    HTable table = new HTable(conf, TABLE_NAME);
    assertEquals(countRows(fs, inputPath), countRows(table));
    table.close();
  }

  @Test
  public void positiveTallTest() throws Exception {
    String args[] = { "-tall", TABLE_NAME, INPUT_PATH };
    WikistatsOnlineLoader loader = new WikistatsOnlineLoader();
    loader.setConf(conf);
    int ret = loader.run(args);

    // verify job existed cleanly
    assertEquals(0, ret);

    // verify the number of records written, by counting
    Path inputPath = new Path(INPUT_PATH);
    FileSystem fs = FileSystem.get(conf);
    HTable table = new HTable(conf, TABLE_NAME);
    assertEquals(countRows(fs, inputPath), countRows(table));
    table.close();
  }
}
