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

package org.springframework.kafka.retrytopic;

import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.util.Assert;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

public class ListenerContainerFactoryConfigurer {

	private static Set<ConcurrentKafkaListenerContainerFactory<?, ?>> configuredFactoriesCache;
	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	static {
		configuredFactoriesCache = new HashSet<>();
	}

	private final static String INTERNAL_KAFKA_CONSUMER_BACKOFF_BEAN_NAME = "kafkaconsumerbackoff-internal";
	private final static long DEFAULT_IDLE_PARTITION_EVENT_INTERVAL = 1000L;
	private final DeadLetterPublishingRecovererProvider deadLetterPublishingRecovererProvider;
	private Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer = container -> {};

	ListenerContainerFactoryConfigurer(KafkaConsumerBackoffManager kafkaConsumerBackoffManager, DeadLetterPublishingRecovererProvider deadLetterPublishingRecovererProvider) {
		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.deadLetterPublishingRecovererProvider = deadLetterPublishingRecovererProvider;
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> configure(ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory, DeadLetterPublishingRecovererProvider.Configuration configuration) {
		if (configuredFactoriesCache.contains(containerFactory)) {
			return containerFactory;
		}
		containerFactory.setContainerCustomizer(container -> setupBackoffAwareMessageListenerAdapter(container));
		containerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		containerFactory.setErrorHandler(createErrorHandler(this.deadLetterPublishingRecovererProvider.create(configuration)));
		configuredFactoriesCache.add(containerFactory);
		return containerFactory;
	}

	protected ErrorHandler createErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
		SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, new FixedBackOff(0, 0));
		errorHandler.setCommitRecovered(true);
		return errorHandler;
	}

	public void addContainerCustomizer(Consumer<ConcurrentMessageListenerContainer<?, ?>> containerCustomizer) {
		this.containerCustomizer = containerCustomizer;
	}

	protected void setupBackoffAwareMessageListenerAdapter(ConcurrentMessageListenerContainer<?, ?> container) {
		AcknowledgingConsumerAwareMessageListener<?, ?> listener = checkAndCast(container.getContainerProperties().getMessageListener(),
				AcknowledgingConsumerAwareMessageListener.class);
		if (container.getContainerProperties().getIdlePartitionEventInterval() == null) {
			container.getContainerProperties().setIdlePartitionEventInterval(DEFAULT_IDLE_PARTITION_EVENT_INTERVAL);
		}
		container.setupMessageListener(new KafkaBackoffAwareMessageListenerAdapter<>(listener, this.kafkaConsumerBackoffManager, container.getListenerId()));
		containerCustomizer.accept(container);
	}

	@SuppressWarnings("unchecked")
	private <T> T checkAndCast(Object obj, Class<T> clazz) {
		Assert.isAssignable(clazz, obj.getClass(),
				() -> String.format("The provided class %s is not assignable from %s", obj.getClass().getSimpleName(), clazz.getSimpleName()));
		return (T) obj;
	}
}
