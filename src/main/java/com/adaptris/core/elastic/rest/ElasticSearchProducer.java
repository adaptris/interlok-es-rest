package com.adaptris.core.elastic.rest;

import java.util.concurrent.TimeUnit;

import com.adaptris.core.CoreException;
import com.adaptris.core.RequestReplyProducerImp;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.util.TimeInterval;

/**
 * Base class for ElasticSearch based activities.
 * 
 * @author lchan
 *
 */
public abstract class ElasticSearchProducer extends RequestReplyProducerImp {

  private static final TimeInterval TIMEOUT = new TimeInterval(2L, TimeUnit.MINUTES);

  public ElasticSearchProducer() {}


  @Override
  public void close() {
    // NOP
  }

  @Override
  public void init() throws CoreException {
    // NOP
  }

  @Override
  public void start() throws CoreException {
    // NOP
  }

  @Override
  public void stop() {
    // NOP
  }

  @Override
  public void prepare() throws CoreException {
    // NOP
  }

  protected long defaultTimeout() {
    return TIMEOUT.toMilliseconds();
  }

  /**
   * @deprecated since 3.8.1, just use {@link CloseableIterable#ensureCloseable(Iterable)} instead.
   */
  @Deprecated
  protected static <E> CloseableIterable<E> ensureCloseable(final Iterable<E> iter) {
    return CloseableIterable.ensureCloseable(iter);
  }
}
