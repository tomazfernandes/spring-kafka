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

import java.time.Duration;

import org.apache.kafka.clients.producer.ProducerRecord;

import org.springframework.kafka.annotation.DelayedTopic;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.Message;
import org.springframework.util.concurrent.ListenableFuture;


/**
 * Delayed operations. Note that to have maximum delay precision, the same amount
 * of delay should be applied to a given partition or topic.
 * It's up to the consumer to respect the delay semantics.
 *
 * @param <K> the key type.
 * @param <V> the outbound data type.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 * @see DelayedTopic
 */
public interface DelayedKafkaOperations<K, V> {

	/**
	 * Sends a message that won't be processed before the given delay.
	 * @param message the message.
	 * @param delay the delay.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDelayed(Message<?> message, Duration delay);

	/**
	 * Sends a {@link ProducerRecord} that won't be processed before the given delay.
	 * @param record the record.
	 * @param delay the delay.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDelayed(ProducerRecord<K, V> record, Duration delay);

	/**
	 * Sends a message to the given topic that won't be processed before
	 * the given delay.
	 * @param topic the topic.
	 * @param data the data.
	 * @param delay the delay.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDelayed(String topic, V data, Duration delay);

	/**
	 * Sends a message to the given topic and partition that won't be processed before
	 * the given delay.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @param delay the delay.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDelayed(String topic, Integer partition, K key, V data, Duration delay);

	/**
	 * Sends a message that won't be processed before the given timestamp.
	 * @param message the message.
	 * @param timestamp the timestamp.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDueAfter(Message<?> message, long timestamp);

	/**
	 * Sends a record that won't be processed before the given timestamp.
	 * @param record the record.
	 * @param timestamp the timestamp.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDueAfter(ProducerRecord<K, V> record, long timestamp);

	/**
	 * Sends a message to the given topic that won't be processed before
	 * the given timestamp.
	 * @param topic the topic.
	 * @param data the data.
	 * @param timestamp the timestamp.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDueAfter(String topic, V data, long timestamp);

	/**
	 * Sends a message to the given topic and partition that won't be processed before
	 * the given timestamp.
	 * @param topic the topic.
	 * @param partition the partition.
	 * @param key the key.
	 * @param data the data.
	 * @param timestamp the timestamp.
	 * @return the result future.
	 */
	ListenableFuture<SendResult<K, V>> sendDueAfter(String topic, Integer partition, K key, V data, long timestamp);
}
