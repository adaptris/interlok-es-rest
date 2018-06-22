package com.adaptris.core.elastic.rest;

import java.util.ArrayList;
import java.util.List;

import javax.mail.URLName;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;

import com.adaptris.core.CoreException;

public class ElasticSearchRestClientCreator implements ElasticSearchClientCreator {
  
  private static final String DEFAULT_SCHEME = "http";

  @Override
  public TransportClient createTransportClient(List<String> transportUrls) throws CoreException {
    TransportClient tClient = new TransportClient();
    
    List<HttpHost> hosts = new ArrayList<>();
    for(String transportUrl : transportUrls) {
      hosts.add(new HttpHost(this.getHost(transportUrl), this.getPort(transportUrl), this.getScheme(transportUrl)));
    }
    
    RestClientBuilder restClientBuilder = RestClient.builder(hosts.toArray(new HttpHost[0]));
    RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
    
    Sniffer sniffer = Sniffer.builder(client.getLowLevelClient()).build();
    
    tClient.setRestHighLevelClient(client);
    tClient.setSniffer(sniffer);
    
    return tClient;
  }

  private String getHost(String hostUrl) {
    String result = hostUrl;
    if (hostUrl.contains("://")) {
      result = new URLName(hostUrl).getHost();
    }
    else {
      result = hostUrl.substring(0, hostUrl.lastIndexOf(":"));
    }
    return result;
  }

  private Integer getPort(String hostUrl) {
    Integer result = 0;
    if (hostUrl.contains("://")) {
      result = new URLName(hostUrl).getPort();
    }
    else {
      String s = hostUrl.substring(hostUrl.lastIndexOf(":") + 1);
      s.replaceAll("/", "");
      result = Integer.parseInt(s);
    }
    return result;
  }
  
  private String getScheme(String hostUrl) {
    String result = DEFAULT_SCHEME;
    if (hostUrl.contains("://"))
      result = hostUrl.substring(0, hostUrl.indexOf("://"));
    
    return result;
  }
}
