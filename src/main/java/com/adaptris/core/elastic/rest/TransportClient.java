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

  private transient RestHighLevelClient restHighLevelClient;

  private transient Sniffer sniffer;

  @Override
  @SuppressWarnings("deprecation")
  public void close() {
    IOUtils.closeQuietly(getSniffer());
    IOUtils.closeQuietly(getRestHighLevelClient());
  }

  public RestHighLevelClient getRestHighLevelClient() {
    return restHighLevelClient;
  }

  private void setRestHighLevelClient(RestHighLevelClient restHighLevelClient) {
    this.restHighLevelClient = restHighLevelClient;
  }

  public TransportClient withRestHighLevelClient(RestHighLevelClient c) {
    setRestHighLevelClient(c);
    return this;
  }

  public Sniffer getSniffer() {
    return sniffer;
  }

  private void setSniffer(Sniffer sniffer) {
    this.sniffer = sniffer;
  }

  public TransportClient withSniffer(Sniffer c) {
    setSniffer(c);
    return this;
  }

  public IndexResponse index(IndexRequest request) throws IOException {
    return this.getRestHighLevelClient().index(request, RequestOptions.DEFAULT);
  }

  public BulkResponse bulk(BulkRequest bulkRequest) throws IOException {
    return this.getRestHighLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
  }

}
