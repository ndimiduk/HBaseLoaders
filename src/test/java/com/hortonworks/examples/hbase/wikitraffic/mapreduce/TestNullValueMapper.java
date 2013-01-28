package com.hortonworks.examples.hbase.wikitraffic.mapreduce;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestNullValueMapper {

  @Test
  public void invokeAsExpected() throws IOException, InterruptedException {
    Text inputKey = new Text("some row key");
    Text inputValue = new Text("some value");

    NullValueMapper<Text, Text> m = new NullValueMapper<Text, Text>();
    @SuppressWarnings("unchecked")
    Mapper<Text, Text, Text, NullWritable>.Context context = mock(Context.class);

    m.map(inputKey, inputValue, context);
    verify(context).write(inputKey, NullWritable.get());
  }
}
