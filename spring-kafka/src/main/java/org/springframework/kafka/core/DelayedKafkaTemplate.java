/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.core;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;

import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

/**
 *
 * A {@link KafkaTemplate} with delay semantics.
 * Note that due to Kafka's ordering guarantees the same delay value should be applied
 * by partition, or topic. Otherwise, a record set to be processed further in the future
 * might block consumption of records that should be processed earlier.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public class DelayedKafkaTemplate<K, V> extends KafkaTemplate<K, V> implements DelayedKafkaOperations<K, V> {

	private String dueTimestampHeader = KafkaHeaders.DUE_TIMESTAMP;

	public DelayedKafkaTemplate(ProducerFactory<K, V> producerFactory) {
		super(producerFactory);
	}

	public DelayedKafkaTemplate(ProducerFactory<K, V> producerFactory, Map<String, Object> configOverrides) {
		super(producerFactory, configOverrides);
	}

	public DelayedKafkaTemplate(ProducerFactory<K, V> producerFactory, boolean autoFlush) {
		super(producerFactory, autoFlush);
	}

	public DelayedKafkaTemplate(ProducerFactory<K, V> producerFactory, boolean autoFlush, Map<String, Object> configOverrides) {
		super(producerFactory, autoFlush, configOverrides);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDelayed(Message<?> message, Duration delay) {
		return sendDueAfter(message, getDueTimestampFor(delay));
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDelayed(ProducerRecord<K, V> record, Duration delay) {
		return sendDueAfter(record, getDueTimestampFor(delay));
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDelayed(String topic, Integer partition, K key, V data, Duration delay) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, partition, key, data);
		addTimestampHeader(producerRecord, getDueTimestampFor(delay));
		return send(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDelayed(String topic, V data, Duration delay) {
		ProducerRecord<K, V> producerRecord = new ProducerRecord<>(topic, data);
		addTimestampHeader(producerRecord, getDueTimestampFor(delay));
		return send(producerRecord);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDueAfter(Message<?> message, long timestamp) {
		addTimestampHeader(message, timestamp);
		return send(message);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDueAfter(ProducerRecord<K, V> record, long timestamp) {
		addTimestampHeader(record, timestamp);
		return send(record);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDueAfter(String topic, V data, long timestamp) {
		ProducerRecord<K, V> record = new ProducerRecord<>(topic, data);
		addTimestampHeader(record, timestamp);
		return send(record);
	}

	@Override
	public ListenableFuture<SendResult<K, V>> sendDueAfter(String topic, Integer partition, K key, V data, long timestamp) {
		ProducerRecord<K, V> record = new ProducerRecord<>(topic, partition, key, data);
		addTimestampHeader(record, timestamp);
		return send(record);
	}

	/**
	 * Set the dueTimestampHeader.
	 * @param dueTimestampHeader the timestamp header.
	 */
	public void setDueTimestampHeader(String dueTimestampHeader) {
		Assert.notNull(dueTimestampHeader, "dueTimestampHeader cannot be null");
		this.dueTimestampHeader = dueTimestampHeader;
	}

	private void addTimestampHeader(Message<?> message, long timestamp) {
		message.getHeaders().put(this.dueTimestampHeader,
				ByteBuffer.allocate(Long.BYTES).putLong(timestamp).array());
	}

	private void addTimestampHeader(ProducerRecord<K, V> record, long timestamp) {
		record.headers().add(this.dueTimestampHeader,
				ByteBuffer.allocate(Long.BYTES).putLong(timestamp).array());
	}

	private long getDueTimestampFor(Duration delay) {
		return Time.SYSTEM.milliseconds() + delay.toMillis();
	}
}
