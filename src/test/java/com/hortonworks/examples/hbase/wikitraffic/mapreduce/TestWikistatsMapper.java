package com.hortonworks.examples.hbase.wikitraffic.mapreduce;

import static com.hortonworks.test.matchers.IsEquivalentPut.isEquivalentPut;
import static org.hamcrest.number.OrderingComparison.comparesEqualTo;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.hortonworks.examples.hbase.wikitraffic.hbase.WikistatsSchemaUtils;

@RunWith(JUnit4.class)
public class TestWikistatsMapper {

  @Test
  public void invokeTallMapper() throws IOException, InterruptedException {
    // setup dummy data
    String dt = "00000000-000000", projectCode = "pc", pageName = "pn";
    long pageViews = 100, bytes = 10000;
    Text inputKey = new Text(String.format("pagecounts-%s.gz:42", dt));
    Text inputValue = new Text(
        String.format("%s %s %d %d", projectCode, pageName, pageViews, bytes));
    Put outputValue = WikistatsSchemaUtils
        .createTallPut(dt, projectCode, pageName, pageViews, bytes);
    ImmutableBytesWritable outputKey = new ImmutableBytesWritable(outputValue.getRow());

    // create our test instance and mocks
    WikistatsMapper m = new WikistatsSchemaUtils.TallWikistatsMapper();
    @SuppressWarnings("unchecked")
    Mapper<Text, Text, ImmutableBytesWritable, Put>.Context context = mock(Context.class);

    m.map(inputKey, inputValue, context);
    verify(context).write(
      argThat(comparesEqualTo(outputKey)),
      argThat(isEquivalentPut(outputValue)));
  }

  @Test
  public void invokeWideMapper() throws IOException, InterruptedException {
    // setup dummy data
    String dt = "00000000-000000", projectCode = "pc", pageName = "pn";
    long pageViews = 100, bytes = 10000;
    Text inputKey = new Text(String.format("pagecounts-%s.gz:42", dt));
    Text inputValue = new Text(
        String.format("%s %s %d %d", projectCode, pageName, pageViews, bytes));
    Put outputValue = WikistatsSchemaUtils
        .createWidePut(dt, projectCode, pageName, pageViews, bytes);
    ImmutableBytesWritable outputKey = new ImmutableBytesWritable(outputValue.getRow());

    // create our test instance and mocks
    WikistatsMapper m = new WikistatsSchemaUtils.WideWikistatsMapper();
    @SuppressWarnings("unchecked")
    Mapper<Text, Text, ImmutableBytesWritable, Put>.Context context = mock(Context.class);

    m.map(inputKey, inputValue, context);
    verify(context).write(
      argThat(comparesEqualTo(outputKey)),
      argThat(isEquivalentPut(outputValue)));
  }
}
