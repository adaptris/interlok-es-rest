package com.adaptris.core.elastic.rest;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.action.index.IndexResponse;
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

public class IndexDocumentsTest extends ProducerCase {

  private static final String EXAMPLE_COMMENT_HEADER = "\n<!--" + "\n-->\n";
  
  private IndexDocuments indexDocuments;
  
  private ElasticSearchConnection elasticSearchConnection;
  
  private AdaptrisMessage adaptrisMessage;
  
  private ProduceDestination produceDestination;
  
  @Mock private TransportClient mockTransportClient;
  
  @Mock private DocumentWrapper mockDocumentWrapper;
  
  @Mock private RequestBuilder mockRequestBuilder;
  
  @Mock private ElasticDocumentBuilder mockElasticDocumentBuilder;
  
  @Mock private IndexResponse mockIndexResponse;
  
  public IndexDocumentsTest(String name) {
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
          when(mockTransportClient.index(any()))
              .thenReturn(mockIndexResponse);
        } catch (IOException e) {
          throw new CoreException(e);
        }
        return mockTransportClient;
      }
    });
    
    when(mockDocumentWrapper.uniqueId())
        .thenReturn(adaptrisMessage.getUniqueId());
    
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper}));
        
    indexDocuments = new IndexDocuments();
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
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockTransportClient).index(any());
  }
  
  public void testMultipleSimpleDocumentIndexed() throws Exception {
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {mockDocumentWrapper, mockDocumentWrapper, mockDocumentWrapper}));
    
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockTransportClient, times(3)).index(any());
  }
  
  public void testNoSimpleDocumentIndexed() throws Exception {
    when(mockElasticDocumentBuilder.build(adaptrisMessage))
        .thenReturn(Arrays.asList(new DocumentWrapper[] {}));
    
    indexDocuments.produce(adaptrisMessage, produceDestination);
    
    verify(mockTransportClient, times(0)).index(any());
  }

  public void testProduceExceptionOnIndexIOException() throws Exception {
    when(mockTransportClient.index(any()))
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

    IndexDocuments producer = new IndexDocuments();
    producer.setDestination(new ConfiguredProduceDestination("myType"));
    producer.setDocumentBuilder(new SimpleDocumentBuilder());
    return new StandaloneProducer(esc, producer);
  }

  @Override
  protected String getExampleCommentHeader(Object o) {
    return super.getExampleCommentHeader(o) + EXAMPLE_COMMENT_HEADER;
  }
  
}
