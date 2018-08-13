package com.adaptris.core.elastic.rest;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;

public interface RequestBuilder {

  public IndexRequest buildIndexRequest(String index, String type, String id, XContentBuilder content);
  
  public UpdateRequest buildUpdateRequest(String index, String type, String id, XContentBuilder content);
  
  public DeleteRequest buildDeleteRequest(String index, String type, String id);
  
  public BulkRequest buildBulkRequest();
  
}
