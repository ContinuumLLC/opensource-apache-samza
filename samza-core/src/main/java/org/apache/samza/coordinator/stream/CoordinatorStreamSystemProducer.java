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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around a SystemProducer that provides helpful methods for dealing
 * with the coordinator stream.
 */
public class CoordinatorStreamSystemProducer {
  private static final Logger log = LoggerFactory.getLogger(CoordinatorStreamSystemProducer.class);

  private final Serde<List<?>> keySerde;
  private final Serde<Map<String, Object>> messageSerde;
  private final SystemStream systemStream;
  private final SystemProducer systemProducer;
  private final SystemAdmin systemAdmin;
  private final String jobNameWithId;
  private boolean isStarted;

  public CoordinatorStreamSystemProducer(SystemStream systemStream, SystemProducer systemProducer, SystemAdmin systemAdmin, String jobNameWithId) {
    this(systemStream, systemProducer, systemAdmin, jobNameWithId, new JsonSerde<List<?>>(), new JsonSerde<Map<String, Object>>());
  }

  public CoordinatorStreamSystemProducer(SystemStream systemStream, SystemProducer systemProducer, SystemAdmin systemAdmin, String jobNameWithId, Serde<List<?>> keySerde, Serde<Map<String, Object>> messageSerde) {
    log.info("CoordinatorStreamSystemProducer [FORK] created for: " + jobNameWithId);
    this.systemStream = systemStream;
    this.systemProducer = systemProducer;
    this.systemAdmin = systemAdmin;
    this.jobNameWithId = jobNameWithId;
    this.keySerde = keySerde;
    this.messageSerde = messageSerde;
  }

  /**
   * Registers a source with the underlying SystemProducer.
   * 
   * @param source
   *          The source to register.
   */
  public void register(String source) {
    systemProducer.register(source);
  }

  /**
   * Creates the coordinator stream, and starts the system producer.
   */
  public void start() {
    if (isStarted) {
      log.info("Coordinator stream producer already started");
      return;
    }
    log.info("Starting coordinator stream producer.");
    systemProducer.start();
    isStarted = true;
  }

  /**
   * Stops the underlying SystemProducer.
   */
  public void stop() {
    log.info("Stopping coordinator stream producer.");
    systemProducer.stop();
    isStarted = false;
  }

  /**
   * Serialize and send a coordinator stream message.
   * 
   * @param message
   *          The message to send.
   */
  public void send(CoordinatorStreamMessage message) {
    log.debug("Sending {}", message);
    try {
      String source = message.getSource();
      List keyList = addJobID(jobNameWithId, message.getKeyArray());
      byte[] key = keySerde.toBytes(keyList);
      byte[] value = null;
      if (!message.isDelete()) {
        value = messageSerde.toBytes(message.getMessageMap());
      }
      OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(systemStream, Integer.valueOf(0), key, value);
      systemProducer.send(source, envelope);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  /**
   * Helper method that sends a series of SetConfig messages to the coordinator
   * stream.
   * 
   * @param source
   *          An identifier to denote which source is sending a message. This
   *          can be any arbitrary string.
   * @param config
   *          The config object to store in the coordinator stream.
   */
  public void writeConfig(String source, Config config) {
    log.debug("Writing config: {}", config);
    for (Map.Entry<String, String> configPair : config.entrySet()) {
      send(new SetConfig(source, configPair.getKey(), configPair.getValue()));
    }
    systemProducer.flush(source);
  }

  protected static List addJobID(String jobNameWithId, Object[] keyArray) {
    ArrayList<Object> keyList = new ArrayList<>(keyArray.length + 1);
    keyList.add(jobNameWithId);
    keyList.addAll(Arrays.asList(keyArray));
    return keyList;
  }
}
