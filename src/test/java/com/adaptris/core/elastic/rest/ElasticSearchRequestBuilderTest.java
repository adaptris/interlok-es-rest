package com.adaptris.core.elastic.rest;

import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.DefaultMessageFactory;

import junit.framework.TestCase;

public class ElasticSearchRequestBuilderTest extends TestCase {
  
  private ElasticSearchRequestBuilder builder;
  
  private AdaptrisMessage adaptrisMessage;
  
  private SimpleDocumentBuilder documentBuilder;
  
  private DocumentWrapper documentWrapper;
  
  public void setUp() throws Exception {
    builder = new ElasticSearchRequestBuilder();
    
    adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage("Some Content");
    
    documentBuilder = new SimpleDocumentBuilder();
    
    Iterable<DocumentWrapper> iterableDocs = documentBuilder.build(adaptrisMessage);
    
    documentWrapper = iterableDocs.iterator().next();
  }
  
  public void testBuildIndexRequest() throws Exception {    
    IndexRequest indexRequest = builder.buildIndexRequest("myIndex", "myType", "myId", documentWrapper.content());
    
    assertEquals("myId", indexRequest.id());
    assertEquals("myIndex", indexRequest.index());
    assertEquals("myType", indexRequest.type());
  }
  
  public void testBuildUpdateRequest() throws Exception {    
    UpdateRequest updateRequest = builder.buildUpdateRequest("myIndex", "myType", "myId", documentWrapper.content());
    
    assertEquals("myId", updateRequest.id());
    assertEquals("myIndex", updateRequest.index());
    assertEquals("myType", updateRequest.type());
  }
  
  public void testBuildDeleteRequest() throws Exception {    
    DeleteRequest deleteRequest = builder.buildDeleteRequest("myIndex", "myType", "myId");
    
    assertEquals("myId", deleteRequest.id());
    assertEquals("myIndex", deleteRequest.index());
    assertEquals("myType", deleteRequest.type());
  }

}
