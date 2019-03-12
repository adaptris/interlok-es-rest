package com.adaptris.core.elastic.rest;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;

public class TransportClient implements Closeable {

  private RestHighLevelClient restHighLevelClient;

  private Sniffer sniffer;

  @Override
  @SuppressWarnings("deprecation")
  public void close() {
    IOUtils.closeQuietly(getSniffer());
    IOUtils.closeQuietly(getRestHighLevelClient());
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
    return this.getRestHighLevelClient().index(request, RequestOptions.DEFAULT);
  }

  public BulkResponse bulk(BulkRequest bulkRequest) throws IOException {
    return this.getRestHighLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
  }

}
