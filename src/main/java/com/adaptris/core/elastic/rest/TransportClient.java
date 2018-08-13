package com.adaptris.core.elastic.rest;

import java.io.IOException;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;

import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.CoreException;

public class TransportClient implements ComponentLifecycle {

  private RestHighLevelClient restHighLevelClient;
  
  private Sniffer sniffer;

  @Override
  public void init() throws CoreException {
  }

  @Override
  public void start() throws CoreException {
  }

  @Override
  public void stop() {
  }

  @Override
  public void close() {
    try {
      if (this.getSniffer() != null)
        this.getSniffer().close();
    }
    catch (Exception e) { ; }
    
    try {
      if (this.getRestHighLevelClient() != null)
        this.getRestHighLevelClient().close();
    }
    catch (Exception e) { ; }
  }

  public RestHighLevelClient getRestHighLevelClient() {
    return restHighLevelClient;
  }

  public void setRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
    this.restHighLevelClient = restHighLevelClient;
  }

  public Sniffer getSniffer() {
    return sniffer;
  }

  public void setSniffer(Sniffer sniffer) {
    this.sniffer = sniffer;
  }

  public IndexResponse index(IndexRequest request) throws IOException {
    return this.getRestHighLevelClient().index(request);
  }

  public BulkResponse bulk(BulkRequest bulkRequest) throws IOException {
    return this.getRestHighLevelClient().bulk(bulkRequest);
  }
  
}
