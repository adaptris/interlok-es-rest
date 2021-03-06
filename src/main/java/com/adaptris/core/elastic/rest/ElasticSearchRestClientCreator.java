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
import com.thoughtworks.xstream.annotations.XStreamAlias;

@XStreamAlias("elasticsearch-rest-client-creator")
public class ElasticSearchRestClientCreator implements ElasticSearchClientCreator {

  private static final String URI_AUTH_SEPARATOR = "://";
  private static final String DEFAULT_SCHEME = "http";

  @Override
  public TransportClient createTransportClient(List<String> transportUrls) throws CoreException {

    List<HttpHost> hosts = new ArrayList<>();
    for(String transportUrl : transportUrls) {
      hosts.add(new HttpHost(this.getHost(transportUrl), this.getPort(transportUrl), this.getScheme(transportUrl)));
    }

    RestClientBuilder restClientBuilder = RestClient.builder(hosts.toArray(new HttpHost[0]));
    RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);

    Sniffer sniffer = Sniffer.builder(client.getLowLevelClient()).build();

    return new TransportClient().withRestHighLevelClient(client).withSniffer(sniffer);
  }

  private String getHost(String hostUrl) {
    String result = hostUrl;
    if (hostUrl.contains(URI_AUTH_SEPARATOR)) {
      result = new URLName(hostUrl).getHost();
    }
    else {
      result = hostUrl.substring(0, hostUrl.lastIndexOf(":"));
    }
    return result;
  }

  private Integer getPort(String hostUrl) {
    Integer result = 0;
    if (hostUrl.contains(URI_AUTH_SEPARATOR)) {
      result = new URLName(hostUrl).getPort();
    }
    else {
      String s = hostUrl.substring(hostUrl.lastIndexOf(":") + 1).replaceAll("/", "");
      result = Integer.parseInt(s);
    }
    return result;
  }

  private String getScheme(String hostUrl) {
    String result = DEFAULT_SCHEME;
    if (hostUrl.contains(URI_AUTH_SEPARATOR))
      result = hostUrl.substring(0, hostUrl.indexOf(URI_AUTH_SEPARATOR));

    return result;
  }
}
