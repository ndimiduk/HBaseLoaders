package com.hortonworks.test.matchers;

import org.apache.hadoop.hbase.client.Put;
import org.hamcrest.Description;
import org.hamcrest.Factory;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class IsEquivalentPut extends TypeSafeMatcher<Put> {

  private final Put put;

  public IsEquivalentPut(Put p) {
    this.put = p;
  }

  @Override
  public void describeTo(Description description) {
    description.appendValue(put);
  }

  @Override
  protected boolean matchesSafely(Put item) {
    return (0 == put.compareTo(item)) &&
        put.getAttributesMap().equals(item.getAttributesMap()) &&
        put.getFamilyMap().equals(item.getFamilyMap());
  }

  @Factory
  public static <T> Matcher<Put> isEquivalentPut(Put p) {
    return new IsEquivalentPut(p);
  }
}
