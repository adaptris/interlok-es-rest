package com.adaptris.core.elastic.rest;

import static org.mockito.Mockito.when;

import java.util.List;

import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.adaptris.core.ComponentLifecycle;
import com.adaptris.core.ConfiguredProduceDestination;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProducerCase;
import com.adaptris.core.StandaloneProducer;
import com.adaptris.core.elastic.SimpleDocumentBuilder;
import com.adaptris.core.util.LifecycleHelper;

public class IndexDocumentsTest extends ProducerCase {

  private static final String EXAMPLE_COMMENT_HEADER = "\n<!--" + "\n-->\n";
  
  private IndexDocuments indexDocuments;
  
  private ElasticSearchConnection elasticSearchConnection;
  
  @Mock private TransportClient mockTransportClient;
  
  @Mock private RestHighLevelClient mockRestHighLevelClient;
  
  @Mock private Sniffer mockSniffer;
  
  public IndexDocumentsTest(String name) {
    super(name);
  }

  // **********************************************************
  // ******* Setup
  //**********************************************************
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    
    elasticSearchConnection = new ElasticSearchConnection();
    elasticSearchConnection.setIndex("myIndex");
    elasticSearchConnection.addTransportUrl("tcp://localhost:9300");
    elasticSearchConnection.setElasticSearchClientCreator(new ElasticSearchClientCreator() {
      
      @Override
      public TransportClient createTransportClient(List<String> transportUrls) throws CoreException {
        when(mockTransportClient.getRestHighLevelClient())
            .thenReturn(mockRestHighLevelClient);
        when(mockTransportClient.getSniffer())
            .thenReturn(mockSniffer);
        
        return mockTransportClient;
      }
    });
    
    indexDocuments = new IndexDocuments();
    indexDocuments.registerConnection(elasticSearchConnection);
    indexDocuments.setDestination(new ConfiguredProduceDestination("myType"));
    
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
  
  public void testNoOp() throws Exception {

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
