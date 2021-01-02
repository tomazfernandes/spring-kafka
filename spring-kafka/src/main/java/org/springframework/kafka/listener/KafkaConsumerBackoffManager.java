/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.listener;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.common.TopicPartition;

import org.springframework.context.ApplicationListener;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;

/**
 *
 * A manager that backs off consumption for a given topic if the timestamp provided is not due. Use with {@link SeekToCurrentErrorHandler}
 * to guarantee that the message is read again after partition consumption is resumed (or seek it manually by other means).
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 * @see SeekToCurrentErrorHandler
 */
public class KafkaConsumerBackoffManager implements ApplicationListener<ListenerContainerPartitionIdleEvent> {

	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(KafkaConsumerBackoffManager.class));

	/**
	 * Default {@link DateTimeFormatter} for the header timestamp.
	 */
	public static final DateTimeFormatter DEFAULT_BACKOFF_TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_DATE_TIME;

	private final KafkaListenerEndpointRegistry registry;
	private final Map<TopicPartition, Context> backOffTimes;

	public KafkaConsumerBackoffManager(KafkaListenerEndpointRegistry registry) {
		this.registry = registry;
		this.backOffTimes = new HashMap<>();
	}

	public void maybeBackoff(Context context) {
		long backoffTime = ChronoUnit.MILLIS.between(LocalDateTime.now(), context.dueTimestamp);
		if (backoffTime > 0) {
			pauseConsumptionAndThrow(context, backoffTime);
		}
	}

	private void pauseConsumptionAndThrow(Context context, Long timeToSleep) throws KafkaBackoffException {
		TopicPartition topicPartition = context.getTopicPartition();
		getListenerContainerFromContext(context).pausePartition(topicPartition);
		this.backOffTimes.put(topicPartition, context);
		throw new KafkaBackoffException(String.format("Partition %s from topic %s is not ready for consumption, backing off.", context.partition(), context.topic()), topicPartition, context.listenerId, context.dueTimestamp().format(DEFAULT_BACKOFF_TIMESTAMP_FORMATTER));
	}

	private MessageListenerContainer getListenerContainerFromContext(Context context) {
		return this.registry.getListenerContainer(context.getListenerId());
	}

	@Override
	public void onApplicationEvent(ListenerContainerPartitionIdleEvent partitionIdleEvent) {
		Context context = this.backOffTimes.get(partitionIdleEvent.getTopicPartition());
		if (context == null || System.currentTimeMillis() < getDueMillis(context)) {
			return;
		}
		MessageListenerContainer container = getListenerContainerFromContext(context);
		container.resumePartition(context.getTopicPartition());
		this.backOffTimes.remove(context.getTopicPartition());
	}

	private long getDueMillis(Context context) {
		return context.dueTimestamp().atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
	}

	public Context createContext(LocalDateTime dueTimestamp, String listenerId, TopicPartition topicPartition) {
		return new Context(dueTimestamp, listenerId, topicPartition);
	}

	public final static class Context {

		/**
		 * The time after which the message can be processed.
		 */
		private final LocalDateTime dueTimestamp;

		/**
		 * The id for the listener that should be paused.
		 */
		private final String listenerId;

		/**
		 * The topic that contains the partition to be paused.
		 */
		private TopicPartition topicPartition;

		private Context(LocalDateTime dueTimestamp, String listenerId, TopicPartition topicPartition) {
			this.dueTimestamp = dueTimestamp;
			this.listenerId = listenerId;
			this.topicPartition = topicPartition;
		}

		public LocalDateTime dueTimestamp() {
			return this.dueTimestamp;
		}

		public TopicPartition getTopicPartition() {
			return this.topicPartition;
		}

		public String topic() {
			return this.topicPartition.topic();
		}

		public int partition() {
			return this.topicPartition.partition();
		}

		public String getListenerId() {
			return this.listenerId;
		}
	}
}
