package com.adaptris.core.elastic.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;

import org.elasticsearch.common.Strings;
import org.junit.Test;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.util.CloseableIterable;
import com.jayway.jsonpath.ReadContext;

public class SimpleJsonDocumentBuilderTest extends BuilderCase {


  @SuppressWarnings("deprecation")
  @Test
  public void testBuild() throws Exception {
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage("Hello World");
    msg.addMetadata(testName.getMethodName(), testName.getMethodName());
    SimpleDocumentBuilder documentBuilder = new SimpleDocumentBuilder();
    int count = 0;
    try (CloseableIterable<DocumentWrapper> docs = ElasticSearchProducer.ensureCloseable(documentBuilder.build(msg))) {
      for (DocumentWrapper doc : docs) {
        count++;
        assertEquals(msg.getUniqueId(), doc.uniqueId());
        ReadContext context = parse(Strings.toString(doc.content()));
        assertEquals("Hello World", context.read("$.content"));
        LinkedHashMap metadata = context.read("$.metadata");
        assertTrue(metadata.containsKey(testName.getMethodName()));
        assertEquals(testName.getMethodName(), metadata.get(testName.getMethodName()));
      }
    }
    assertEquals(1, count);
  }

}
