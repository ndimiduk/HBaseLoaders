package com.hortonworks.examples.hbase.wikitraffic.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestWikistatsSchemaUtils {

  @Test
  public void createTallPut() {
    String dateTime = "dt", projectCode = "pc", pageName = "pn";
    long pageViews = 100, bytes = 10000;
    String rowkey = String.format("%s/%s/%s", projectCode, pageName, dateTime);

    Put p = WikistatsSchemaUtils
        .createTallPut(dateTime, projectCode, pageName, pageViews, bytes);
    Map<byte[], List<KeyValue>> m = p.getFamilyMap();
    assertTrue(m.containsKey(WikistatsSchemaUtils.COLUMN_FAMILY_NAME));

    List<KeyValue> kvs = m.get(WikistatsSchemaUtils.COLUMN_FAMILY_NAME);
    assertEquals(2, kvs.size());

    for (KeyValue kv : kvs) {
      assertEquals(rowkey, Bytes.toString(kv.getRow()));
      if (Arrays.equals(WikistatsSchemaUtils.VIEWS_QUAL, kv.getQualifier())) {
        assertEquals(pageViews, Bytes.toLong(kv.getValue()));
      } else if (Arrays.equals(WikistatsSchemaUtils.BYTES_QUAL, kv.getQualifier())) {
        assertEquals(bytes, Bytes.toLong(kv.getValue()));
      } else {
        fail("Put includes unexpected data.");
      }
    }
  }

  @Test
  public void createWidePut() {
    String dateTime = "dt", projectCode = "pc", pageName = "pn";
    long pageViews = 100, bytes = 10000;
    String rowkey = String.format("%s/%s", projectCode, pageName);
    // TODO: replace this with maps and an actual json writer/parser.
    String json = String.format("{\"views\":\"%s\",\"bytes\":%d", pageViews, bytes);

    Put p = WikistatsSchemaUtils
        .createWidePut(dateTime, projectCode, pageName, pageViews, bytes);
    Map<byte[], List<KeyValue>> m = p.getFamilyMap();
    assertTrue(m.containsKey(WikistatsSchemaUtils.COLUMN_FAMILY_NAME));

    List<KeyValue> kvs = m.get(WikistatsSchemaUtils.COLUMN_FAMILY_NAME);
    assertEquals(1, kvs.size());

    assertEquals(rowkey, Bytes.toString(kvs.get(0).getRow()));
    assertEquals(dateTime, Bytes.toString(kvs.get(0).getQualifier()));
    assertEquals(json, Bytes.toString(kvs.get(0).getValue()));
  }
}
