package com.adaptris.core.elastic.rest;

import java.util.ArrayList;
import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.Size;

import org.hibernate.validator.constraints.NotBlank;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.core.CoreException;
import com.adaptris.core.NoOpConnection;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

/**
 * 
 * @author lchan
 * @config elasticsearch-connection
 */
@XStreamAlias("elasticsearch-rest-connection")
public class ElasticSearchConnection extends NoOpConnection {

  @XStreamImplicit(itemFieldName = "transport-url")
  @Size(min = 1)
  @Valid
  private List<String> transportUrls;

  @NotBlank
  private String index = null;
  
  @AutoPopulated
  @Valid
  private ElasticSearchClientCreator elasticSearchClientCreator;

  public ElasticSearchConnection() {
    setTransportUrls(new ArrayList<String>());
    this.setElasticSearchClientCreator(new ElasticSearchRestClientCreator());
  }

  public ElasticSearchConnection(String index) {
    this();
    setIndex(index);
  }

  protected TransportClient createTransportClient() throws CoreException {
    return this.getElasticSearchClientCreator().createTransportClient(getTransportUrls());
  }

  public List<String> getTransportUrls() {
    return transportUrls;
  }

  public void setTransportUrls(List<String> transports) {
    this.transportUrls = Args.notNull(transports, "Transport URLS");
  }

  public void addTransportUrl(String url) {
    transportUrls.add(Args.notNull(url, "URL"));
  }

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = Args.notBlank(index, "index");
  }

  protected void closeQuietly(TransportClient c) {
    doClose(c);
  }

  private static void doClose(TransportClient c) {
    try {
      if (c != null) {
        c.close();
      }
    }
    catch (Exception e) {
      ;
    }
  }

  public ElasticSearchClientCreator getElasticSearchClientCreator() {
    return elasticSearchClientCreator;
  }

  public void setElasticSearchClientCreator(ElasticSearchClientCreator elasticSearchClientCreator) {
    this.elasticSearchClientCreator = elasticSearchClientCreator;
  }
}
