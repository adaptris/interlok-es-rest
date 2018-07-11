package com.adaptris.core.elastic.rest;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elasticsearch-uppercase-field-name-mapper")
public class ToUpperCaseFieldNameMapper implements FieldNameMapper {

  @Override
  public String map(String name) {
    return name.toUpperCase();
  }

}
