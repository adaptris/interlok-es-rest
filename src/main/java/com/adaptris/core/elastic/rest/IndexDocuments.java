package com.adaptris.core.elastic.rest;

import java.io.IOException;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.core.elastic.ElasticDocumentBuilder;
import com.adaptris.core.elastic.ElasticSearchProducer;
import com.adaptris.core.elastic.SimpleDocumentBuilder;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Add a document(s) to ElasticSearch.
 * 
 * <p>
 * {@link ProduceDestination#getDestination(AdaptrisMessage)} should return the type of document that we are submitting to into
 * ElasticSearch; the {@code index} is taken from the underlying {@link ElasticSearchConnection}.
 * </p>
 * 
 * @author lchan
 * @config elasticsearch-index-document
 *
 */
@XStreamAlias("elasticsearch-rest-index-document")
public class IndexDocuments extends ElasticSearchProducer {

  protected transient TransportClient transportClient = null;

  @Valid
  @NotNull
  @AutoPopulated
  private ElasticDocumentBuilder documentBuilder;

  public IndexDocuments() {
    setDocumentBuilder(new SimpleDocumentBuilder());
  }

  public void produce(AdaptrisMessage msg, ProduceDestination destination) throws ProduceException {
    request(msg, destination, defaultTimeout());
  }


  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout) throws ProduceException {
    try {
      final String type = destination.getDestination(msg);
      final String index = retrieveConnection(ElasticSearchConnection.class).getIndex();
      try (CloseableIterable<DocumentWrapper> docs = ensureCloseable(documentBuilder.build(msg))) {
        docs.forEach(e -> {
          IndexRequest request = new IndexRequest(index, type, e.uniqueId()); 
          request.source(e.content());
          
          IndexResponse response;
          try {
            response = transportClient.getRestHighLevelClient().index(request);
          } catch (IOException e1) {
            throw new RuntimeException(e1);
          }
          log.trace("Added document {} version {} to {}", response.getId(), response.getVersion(), index);
        });
      }
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

  @Override
  public void close() {
    super.close();
    retrieveConnection(ElasticSearchConnection.class).closeQuietly(transportClient);
  }

  @Override
  public void init() throws CoreException {
    super.init();
    transportClient = retrieveConnection(ElasticSearchConnection.class).createTransportClient();
  }

  /**
   * @return the documentBuilder
   */
  public ElasticDocumentBuilder getDocumentBuilder() {
    return documentBuilder;
  }

  /**
   * @param b the documentBuilder to set
   */
  public void setDocumentBuilder(ElasticDocumentBuilder b) {
    this.documentBuilder = b;
  }



}
