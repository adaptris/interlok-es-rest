package com.adaptris.core.elastic.rest;

import javax.validation.constraints.NotNull;

import com.adaptris.core.AdaptrisMessage;
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elasticsearch-metadata-action")
public class MetadataAction implements ActionExtractor {

  @NotNull
  private String metadataKey;

  @Override
  public String extract(AdaptrisMessage msg, DocumentWrapper document) {
    return msg.getMetadataValue(metadataKey());
  }

  public String getMetadataKey() {
    return metadataKey;
  }

  public void setMetadataKey(String metadataKey) {
    this.metadataKey = metadataKey;
  }
  
  private String metadataKey() {
    return getMetadataKey() != null ? getMetadataKey() : "action";
  }

}
