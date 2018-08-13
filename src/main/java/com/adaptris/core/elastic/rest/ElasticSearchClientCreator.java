package com.adaptris.core.elastic.rest;

import java.util.List;

import com.adaptris.core.CoreException;

public interface ElasticSearchClientCreator {

  public TransportClient createTransportClient(List<String> transportUrls) throws CoreException;
  
}
