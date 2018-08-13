package com.adaptris.core.elastic.rest;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elasticsearch-request-builder")
public class ElasticSearchRequestBuilder implements RequestBuilder {

  @Override
  public IndexRequest buildIndexRequest(String index, String type, String id, XContentBuilder content) {
    IndexRequest request = new IndexRequest(index, type, id); 
    request.source(content);
    
    return request;
  }

  @Override
  public UpdateRequest buildUpdateRequest(String index, String type, String id, XContentBuilder content) {
    UpdateRequest updateRequest = new UpdateRequest(index, type, id); 
    updateRequest.upsert(content);
    
    return updateRequest;
  }

  @Override
  public DeleteRequest buildDeleteRequest(String index, String type, String id) {
    return new DeleteRequest(index, type, id); 
  }

  @Override
  public BulkRequest buildBulkRequest() {
    return new BulkRequest();
  }

}
