package com.adaptris.core.elastic.rest;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.DefaultMessageFactory;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.util.LifecycleHelper;

public class BulkIndexDocumentsTest extends ProducerCase {

  private static final String EXAMPLE_COMMENT_HEADER = "\n<!--" + "\n-->\n";
  
  private BulkIndexDocuments indexDocuments;
  
  private ElasticSearchConnection elasticSearchConnection;
  
  private AdaptrisMessage adaptrisMessage;
  
  private ProduceDestination produceDestination;
  
  @Mock private TransportClient mockTransportClient;
  
  @Mock private DocumentWrapper mockDocumentWrapper;
  
  @Mock private RequestBuilder mockRequestBuilder;
  
  @Mock private BulkRequest mockBulkRequest;
  
  @Mock private ElasticDocumentBuilder mockElasticDocumentBuilder;
  
  @Mock private BulkResponse mockBulkResponse;
  
  @Mock private TimeValue mockTimeValue;
  
  @Mock private ActionExtractor mockActionExtractor;
  
  public BulkIndexDocumentsTest(String name) {
    super(name);
  }

  // **********************************************************
  // ******* Setup
  //**********************************************************
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    
    adaptrisMessage = DefaultMessageFactory.getDefaultInstance().newMessage("Some content");
    
    produceDestination = new ConfiguredProduceDestination("MyDestination");
    
    elasticSearchConnection = new ElasticSearchConnection();
    elasticSearchConnection.setIndex("myIndex");
    elasticSearchConnection.addTransportUrl("tcp://localhost:9300");
    elasticSearchConnection.setElasticSearchClientCreator(new ElasticSearchClientCreator() {
      
      @Override
      public TransportClient createTransportClient(List<String> transportUrls) throws CoreException {
        try {
          when(mockTransportClient.bulk(any()))
              .thenReturn(mockBulkResponse);
        } catch (IOException e) {
          throw new CoreException(e);
        }
        return mockTransportClient;
      }
    });
    
    when(mockBulkResponse.hasFailures())
        .thenReturn(false);
    
    when(mockBulkResponse.getTook())
        .thenReturn(mockTimeValue);
    
    when(mockRequestBuilder.buildBulkRequest())
        .thenReturn(mockBulkRequest);
    
    when(mockDocumentWrapper.uniqueId())
        .thenReturn(adaptrisMessage.getUniqueId());
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper}));
        
    indexDocuments = new BulkIndexDocuments();
    indexDocuments.registerConnection(elasticSearchConnection);
    indexDocuments.setDestination(new ConfiguredProduceDestination("myType"));
    indexDocuments.setDocumentBuilder(mockElasticDocumentBuilder);
    indexDocuments.setRequestBuilder(mockRequestBuilder);
    
    startComponent(elasticSearchConnection);
    startComponent(indexDocuments);
  }
  
  public void tearDown() throws Exception {
    stopComponent(elasticSearchConnection);
    stopComponent(indexDocuments);
  }
  
  //**********************************************************
  // ******* Tests
  //**********************************************************
  
  public void testSimpleDocumentIndexed() throws Exception {
    when(mockBulkRequest.numberOfActions())
        .thenReturn(1);
    
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockTransportClient).bulk(any());
  }
  
  public void testMultipleSimpleDocumentIndexed() throws Exception {
    when(mockBulkRequest.numberOfActions())
        .thenReturn(3);
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper, mockDocumentWrapper, mockDocumentWrapper}));
    
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockRequestBuilder, times(3)).buildIndexRequest(any(), any(), any(), any());
    verify(mockTransportClient).bulk(any());
  }
  
  public void testMultipleSimpleDocumentIndexedWithFailures() throws Exception {
    when(mockBulkRequest.numberOfActions())
        .thenReturn(3);
    
    when(mockBulkResponse.hasFailures())
        .thenReturn(true);
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper, mockDocumentWrapper, mockDocumentWrapper}));
    
    try {
      indexDocuments.produce(adaptrisMessage, produceDestination);
      fail("One of the documents failed, should throw PE");
    } catch (ProduceException ex) {
      // expected
    }
  }
  
  public void testMultipleSimpleDocumentIndexedMultiBatch() throws Exception {
    when(mockBulkRequest.numberOfActions())
        .thenReturn(1)
        .thenReturn(1)
        .thenReturn(1)
        .thenReturn(0);
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper, mockDocumentWrapper, mockDocumentWrapper}));
    
    indexDocuments.setBatchWindow(0);
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockRequestBuilder, times(3)).buildIndexRequest(any(), any(), any(), any());
    verify(mockTransportClient, times(3)).bulk(any());
  }
  
  public void testMultipleSimpleDocumentDifferentActions() throws Exception {
    when(mockActionExtractor.extract(any(), any()))
        .thenReturn("INDEX")
        .thenReturn("DELETE")
        .thenReturn("UPDATE");
    
    when(mockBulkRequest.numberOfActions())
        .thenReturn(3);
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper, mockDocumentWrapper, mockDocumentWrapper}));
    
    indexDocuments.setAction(mockActionExtractor);
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockRequestBuilder, times(1)).buildIndexRequest(any(), any(), any(), any());
    verify(mockRequestBuilder, times(1)).buildUpdateRequest(any(), any(), any(), any());
    verify(mockRequestBuilder, times(1)).buildDeleteRequest(any(), any(), any());
    verify(mockTransportClient).bulk(any());
  }
  
  public void testIncorrectAction() throws Exception {
    when(mockActionExtractor.extract(any(), any()))
        .thenReturn("DOES_NOT_EXIST");
    
    when(mockBulkRequest.numberOfActions())
        .thenReturn(1);
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper}));
    
    indexDocuments.setAction(mockActionExtractor);
    try {
      indexDocuments.produce(adaptrisMessage, produceDestination);
      fail("Malformed action, should fail.");
    } catch (ProduceException ex) {
      // expected.
    }
  }
  
  public void testNoSimpleDocumentIndexed() throws Exception {
    when(mockBulkRequest.numberOfActions())
        .thenReturn(0);
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {}));
    
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockTransportClient, times(0)).bulk(any());
  }

  public void testProduceExceptionOnIndexIOException() throws Exception {
    when(mockBulkRequest.numberOfActions())
        .thenReturn(1);
    when(mockTransportClient.bulk(any()))
        .thenThrow(new IOException("expected"));
    
    try {
      indexDocuments.produce(adaptrisMessage, produceDestination);
      fail("Expected a produce exception wrapping the IOEx");
    } catch (ProduceException ex) {
      //expected.
    }
  }
  
  //**********************************************************
  // ******* Util
  //**********************************************************
  
  private void startComponent(ComponentLifecycle component) throws Exception {
    LifecycleHelper.init(component);
    LifecycleHelper.start(component);
  }
  
  private void stopComponent(ComponentLifecycle component) throws Exception {
    LifecycleHelper.stop(component);
    LifecycleHelper.close(component);
  }
  
  //**********************************************************
  // ******* Example XML
  //**********************************************************
  
  @Override
  protected Object retrieveObjectForSampleConfig() {
    ElasticSearchConnection esc = new ElasticSearchConnection("myIndex");
    esc.addTransportUrl("localhost:9300");
    esc.addTransportUrl("localhost:9301");
    esc.addTransportUrl("localhost:9302");

    BulkIndexDocuments producer = new BulkIndexDocuments();
    producer.setDestination(new ConfiguredProduceDestination("myType"));
    producer.setDocumentBuilder(new SimpleDocumentBuilder());
    producer.setBatchWindow(10);
    return new StandaloneProducer(esc, producer);
  }

  @Override
  protected String getExampleCommentHeader(Object o) {
    return super.getExampleCommentHeader(o) + EXAMPLE_COMMENT_HEADER;
  }
  
}