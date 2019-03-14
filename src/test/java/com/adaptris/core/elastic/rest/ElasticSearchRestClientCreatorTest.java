package com.adaptris.core.elastic.rest;

import static org.junit.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class ElasticSearchRestClientCreatorTest {

  @Test
  public void testCreate() throws Exception {
    ElasticSearchRestClientCreator creator = new ElasticSearchRestClientCreator();
    TransportClient client = creator.createTransportClient(getTransportUrls());
    assertNotNull(client);
    client.close();
  }

  private List<String> getTransportUrls() {
    return Arrays.asList("http://localhost:9200", "localhost:9201", "https://localhost:9202");
  }
}
