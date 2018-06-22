package com.adaptris.core.elastic.rest;

import javax.validation.Valid;
import javax.validation.constraints.Min;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;

import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ProduceDestination;
import com.adaptris.core.ProduceException;
import com.adaptris.core.elastic.ActionExtractor;
import com.adaptris.core.elastic.ConfiguredAction;
import com.adaptris.core.elastic.DocumentAction;
import com.adaptris.core.elastic.DocumentWrapper;
import com.adaptris.core.elastic.ElasticSearchConnection;
import com.adaptris.core.elastic.rest.IndexDocuments;
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
 * @config elasticsearch-bulk-index-document
 *
 */
@XStreamAlias("elasticsearch-rest-bulk-index-document")
public class BulkIndexDocuments extends IndexDocuments {

  private static final int DEFAULT_BATCH_WINDOW = 10000;

  @Min(0)
  private Integer batchWindow;
  
  @AdvancedConfig
  @Valid
  private ActionExtractor action;

  public BulkIndexDocuments() {
    super();
    ConfiguredAction ca = new ConfiguredAction();
    ca.setAction(DocumentAction.INDEX);
    setAction(ca);
  }

  @Override
  protected AdaptrisMessage doRequest(AdaptrisMessage msg, ProduceDestination destination, long timeout) throws ProduceException {
    try {
      final String type = destination.getDestination(msg);
      final String index = retrieveConnection(ElasticSearchConnection.class).getIndex();
      BulkRequest bulkRequest = new BulkRequest();
      try (CloseableIterable<DocumentWrapper> docs = ensureCloseable(getDocumentBuilder().build(msg))) {
        int count = 0;
        for (DocumentWrapper doc : docs) {
          count++;
          DocumentAction action = DocumentAction.valueOf(getAction().extract(msg, doc));
          switch(action) {
          case INDEX:
            IndexRequest indexRequest = new IndexRequest(index, type, doc.uniqueId()); 
            indexRequest.source(doc.content());
            
            bulkRequest.add(indexRequest);
            break;
          case UPDATE:
            UpdateRequest updateRequest = new UpdateRequest(index, type, doc.uniqueId()); 
            updateRequest.upsert(doc.content());
            
            bulkRequest.add(updateRequest);
            break;
          case DELETE:
            DeleteRequest deleteRequest = new DeleteRequest(index, type, doc.uniqueId()); 
            bulkRequest.add(deleteRequest);
            break;
          default:
            throw new ProduceException("Unrecognized action: " + action);
          }
          if (count >= batchWindow()) {
            doSend(bulkRequest);
            count = 0;
          }
        }
      }
      if (bulkRequest.numberOfActions() > 0) {
        doSend(bulkRequest);
      }
    }
    catch (Exception e) {
      throw ExceptionHelper.wrapProduceException(e);
    }
    return msg;
  }

  private void doSend(BulkRequest bulkRequest) throws Exception {
    int count = bulkRequest.numberOfActions();
    BulkResponse response = transportClient.getRestHighLevelClient().bulk(bulkRequest);
    if (response.hasFailures()) {
      throw new ProduceException(response.buildFailureMessage());
    }
    log.trace("Producing batch of {} actions took {}", count, response.getTook().toString());
    return;
  }


  /**
   * @return the batchCount
   */
  public Integer getBatchWindow() {
    return batchWindow;
  }

  /**
   * @param b the batchCount to set
   */
  public void setBatchWindow(Integer b) {
    this.batchWindow = b;
  }

  int batchWindow() {
    return getBatchWindow() != null ? getBatchWindow().intValue() : DEFAULT_BATCH_WINDOW;
  }


  public ActionExtractor getAction() {
    return action;
  }


  public void setAction(ActionExtractor action) {
    this.action = action;
  }

}
