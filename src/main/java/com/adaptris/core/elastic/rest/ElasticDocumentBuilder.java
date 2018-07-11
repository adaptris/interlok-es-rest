package com.adaptris.core.elastic.rest;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceException;

public interface ElasticDocumentBuilder {

  Iterable<DocumentWrapper> build(AdaptrisMessage msg) throws ProduceException;
  
}
