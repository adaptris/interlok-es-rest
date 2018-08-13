package com.adaptris.core.elastic.rest;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elasticsearch-noop-field-name-mapper")
public class NoOpFieldNameMapper implements FieldNameMapper {

  @Override
  public String map(String name) {
    return name;
  }

}
