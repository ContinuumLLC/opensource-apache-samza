/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.coordinator.stream;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin;
import org.apache.samza.util.Util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.assertNotNull;


/**
 * Helper for creating mock CoordinatorStreamConsumer and
 * CoordinatorStreamConsumer. The CoordinatorStreamConsumer is meant to just
 * forward all configs to JobCoordinator, which is useful for mocking in unit
 * tests.
 */
public class MockCoordinatorStreamSystemFactory implements SystemFactory {

  private static SystemConsumer mockConsumer = null;
  private static boolean useCachedConsumer = false;

  public static void enableMockConsumerCache() {
    mockConsumer = null;
    useCachedConsumer = true;
  }

  public static void disableMockConsumerCache() {
    useCachedConsumer = false;
    mockConsumer = null;
  }

  public static CoordinatorStreamMessage deserializeCoordinatorStreamMessage(OutgoingMessageEnvelope msg) {
    JsonSerde<List<?>> keySerde = new JsonSerde<>();
    Object[] keyArray = keySerde.fromBytes((byte[]) msg.getKey()).toArray();
    Object[] newKeyArray = CoordinatorStreamSystemConsumer.dropJobID(keyArray);
    JsonSerde<Map<String, Object>> msgSerde = new JsonSerde<>();
    Map<String, Object> valueMap = msgSerde.fromBytes((byte[]) msg.getMessage());
    return new CoordinatorStreamMessage(newKeyArray, valueMap);
  }

  public static OutgoingMessageEnvelope convertOutEnv(OutgoingMessageEnvelope envelope) {
    Object newKey = convertKey(envelope.getKey(), keyArray -> Arrays.asList(CoordinatorStreamSystemConsumer.dropJobID(keyArray)));
    return new OutgoingMessageEnvelope(
            envelope.getSystemStream(),
            envelope.getKeySerializerName(),
            envelope.getMessageSerializerName(),
            envelope.getPartitionKey(),
            newKey,
            envelope.getMessage()
    );
  }

  public static IncomingMessageEnvelope convertInEnv(String jobNameAndID, IncomingMessageEnvelope envelope) {
    Object newKey = convertKey(envelope.getKey(), keyArray -> CoordinatorStreamSystemProducer.addJobID(jobNameAndID, keyArray));
    return new IncomingMessageEnvelope(
            envelope.getSystemStreamPartition(),
            envelope.getOffset(),
            newKey,
            envelope.getMessage(),
            envelope.getSize()
    );
  }

  private static Object convertKey(Object key, Function<Object[], List> func) {
    JsonSerde<List<?>> keySerde = new JsonSerde<>();
    Object[] keyArray = keySerde.fromBytes((byte[]) key).toArray();
    List newKeyList = func.apply(keyArray);
    return keySerde.toBytes(newKeyList);
  }

  /**
   * Returns a consumer that sends all configs to the coordinator stream.
   *
   * @param config Along with the configs, you can pass checkpoints and changelog stream messages into the stream.
   *               The expected pattern is cp:source:taskname -> ssp,offset for checkpoint (Use sspToString util)
   *               ch:source:taskname -> changelogPartition for changelog
   *               Everything else is processed as normal config
   */
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {

    if (useCachedConsumer && mockConsumer != null) {
      return mockConsumer;
    }

    String streamName = Util.getCoordinatorStreamName(config);
    SystemStreamPartition systemStreamPartition = new SystemStreamPartition(systemName, streamName, new Partition(0));
    mockConsumer = new MockCoordinatorStreamWrappedConsumer(systemStreamPartition, config);
    return mockConsumer;
  }

  private SystemStream getCoordinatorSystemStream(Config config) {
    assertNotNull(config.get("job.coordinator.system"));
    assertNotNull(config.get("job.coordinator.topic"));
    return new SystemStream(config.get("job.coordinator.system"), Util.getCoordinatorStreamName(config));
  }

  /**
   * Returns a MockCoordinatorSystemProducer.
   */
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new MockSystemProducer(null);
  }

  public MockCoordinatorStreamSystemConsumer getCoordinatorStreamSystemConsumer(Config config, MetricsRegistry registry) {
    return new MockCoordinatorStreamSystemConsumer(getCoordinatorSystemStream(config),
        getConsumer(config.get("job.coordinator.system"), config, registry),
        getAdmin(config.get("job.coordinator.system"), config),
        Util.getJobNameWithId(config));
  }

  public MockCoordinatorStreamSystemProducer getCoordinatorStreamSystemProducer(Config config, MetricsRegistry registry) {
    return new MockCoordinatorStreamSystemProducer(getCoordinatorSystemStream(config),
        getProducer(config.get("job.coordinator.system"), config, registry),
        getAdmin(config.get("job.coordinator.system"), config),
        Util.getJobNameWithId(config));
  }

  /**
   * Returns a single partition admin that pretends to create a coordinator
   * stream.
   */
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new MockSystemAdmin();
  }

  public static final class MockCoordinatorStreamSystemConsumer extends CoordinatorStreamSystemConsumer {
    private final MockCoordinatorStreamWrappedConsumer consumer;
    private final SystemStream stream;
    private boolean isRegistered = false;
    private boolean isStarted = false;

    public MockCoordinatorStreamSystemConsumer(SystemStream stream, SystemConsumer consumer, SystemAdmin admin, String jobNameWithId) {
      super(stream, consumer, admin, jobNameWithId);
      this.stream = stream;
      this.consumer = (MockCoordinatorStreamWrappedConsumer) consumer;
    }

    public MockCoordinatorStreamWrappedConsumer getConsumer() {
      return this.consumer;
    }

    public void register() {
      SystemStreamPartition ssp = new SystemStreamPartition(stream, new Partition(0));
      consumer.register(ssp, "");
      isRegistered = true;
    }

    public void start() {
      isStarted = true;
    }

    public void stop() {
      isStarted = false;
    }

    public boolean isRegistered() {
      return isRegistered;
    }

    public boolean isStarted() {
      return isStarted;
    }

    public boolean isStopped() {
      return !isStarted;
    }
  }

  public static final class MockCoordinatorStreamSystemProducer extends CoordinatorStreamSystemProducer {
    private final MockSystemProducer producer;

    public MockCoordinatorStreamSystemProducer(SystemStream stream, SystemProducer producer, SystemAdmin admin, String jobNameWithId) {
      super(stream, producer, admin, jobNameWithId);
      this.producer = (MockSystemProducer) producer;
    }

    public boolean isRegistered() {
      return this.producer.isRegistered();
    }

    public String getRegisteredSource() {
      return this.producer.getRegisteredSource();
    }

    public boolean isStarted() {
      return this.producer.isStarted();
    }

    public boolean isStopped() {
      return this.producer.isStopped();
    }

    public List<OutgoingMessageEnvelope> getEnvelopes() {
      return this.producer.getEnvelopes();
    }
  }

  public static final class MockSystemAdmin extends SinglePartitionWithoutOffsetsSystemAdmin implements SystemAdmin {
    public void createCoordinatorStream(String streamName) {
      // Do nothing.
    }
  }

  protected static class MockSystemProducer implements SystemProducer {
    private final String expectedSource;
    private final List<OutgoingMessageEnvelope> envelopes;
    private boolean started = false;
    private boolean registered = false;
    private String registeredSource = null;
    private boolean flushed = false;

    public MockSystemProducer(String expectedSource) {
      this.expectedSource = expectedSource;
      this.envelopes = new ArrayList<>();
    }


    public void start() {
      started = true;
    }

    public void stop() {
      started = false;
    }

    public void register(String source) {
      registered = true;
      registeredSource = source;
    }

    public void send(String source, OutgoingMessageEnvelope envelope) {
      if (mockConsumer != null) {
        MockCoordinatorStreamWrappedConsumer consumer = (MockCoordinatorStreamWrappedConsumer) mockConsumer;
        SystemStreamPartition ssp = new SystemStreamPartition(envelope.getSystemStream(), new Partition(0));
        consumer.register(ssp, "");
        try {
          consumer.addMessageEnvelope(new IncomingMessageEnvelope(ssp, "", envelope.getKey(), envelope.getMessage()));
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
        }
      } else {
        envelopes.add(envelope);
      }
    }

    public void flush(String source) {
      flushed = true;
    }

    public List<OutgoingMessageEnvelope> getEnvelopes() {
      return envelopes;
    }

    public boolean isStarted() {
      return started;
    }

    public boolean isStopped() {
      return !started;
    }

    public boolean isRegistered() {
      return registered;
    }

    public boolean isFlushed() {
      return flushed;
    }

    public String getExpectedSource() {
      return expectedSource;
    }

    public String getRegisteredSource() {
      return registeredSource;
    }
  }
}
