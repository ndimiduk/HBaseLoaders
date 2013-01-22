package com.hortonworks.examples.hbase.wikitraffic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class NullValueMapper<K, V> extends Mapper<K, V, K, NullWritable> {

  @Override
  public void map(K key, V value, Context context)
      throws IOException, InterruptedException {
    context.write(key, NullWritable.get());
  }
}
