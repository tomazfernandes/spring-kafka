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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.NestedRuntimeException;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicResolver;
import org.springframework.util.Assert;
import org.springframework.util.backoff.FixedBackOff;

public class ListenerContainerFactoryConfigurer {

	private static Set<ConcurrentKafkaListenerContainerFactory<?, ?>> configuredFactoriesCache;

	static {
		configuredFactoriesCache = new HashSet<>();
	}

	private final static String INTERNAL_KAFKA_CONSUMER_BACKOFF_BEAN_NAME = "kafkaconsumerbackoff-internal";
	private final DeadLetterPublishingRecovererProvider deadLetterPublishingRecovererProvider;
	private BeanFactory beanFactory;

	ListenerContainerFactoryConfigurer(Map<Class<?>, KafkaOperations<?, ?>> templates, BinaryExceptionClassifier exceptionClassifier, DestinationTopicResolver destinationTopicResolver) {
		this.deadLetterPublishingRecovererProvider = new DeadLetterPublishingRecovererProvider(templates, exceptionClassifier, destinationTopicResolver);
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> configure(ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory) {
		if (configuredFactoriesCache.contains(containerFactory)) {
			return containerFactory;
		}
		containerFactory.setContainerCustomizer(this::setupBackoffAwareMessageListenerAdapter);
		containerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		containerFactory.setErrorHandler(createErrorHandler(this.deadLetterPublishingRecovererProvider.create()));
		configuredFactoriesCache.add(containerFactory);
		return containerFactory;
	}

	protected ErrorHandler createErrorHandler(DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
		SeekToCurrentErrorHandler errorHandler = new SeekToCurrentErrorHandler(deadLetterPublishingRecoverer, new FixedBackOff(0, 0));
		errorHandler.setCommitRecovered(true);
		return errorHandler;
	}

	protected void setupBackoffAwareMessageListenerAdapter(ConcurrentMessageListenerContainer<?, ?> container) {
		AcknowledgingConsumerAwareMessageListener<?, ?> listener = checkAndCast(container.getContainerProperties().getMessageListener(),
				AcknowledgingConsumerAwareMessageListener.class);
		KafkaConsumerBackoffManager kafkaConsumerBackoffManager = getKafkaConsumerBackoff();
		if (container.getContainerProperties().getIdlePartitionEventInterval() == null) {
			container.getContainerProperties().setIdlePartitionEventInterval(1000L);
		}
		container.setupMessageListener(new KafkaBackoffAwareMessageListenerAdapter<>(listener, kafkaConsumerBackoffManager, container.getListenerId()));
	}

	private KafkaConsumerBackoffManager getKafkaConsumerBackoff() {
		return this.beanFactory.containsBean(INTERNAL_KAFKA_CONSUMER_BACKOFF_BEAN_NAME)
				? this.beanFactory.getBean(INTERNAL_KAFKA_CONSUMER_BACKOFF_BEAN_NAME, KafkaConsumerBackoffManager.class)
				: createKafkaConsumerBackoffBean();
	}

	private KafkaConsumerBackoffManager createKafkaConsumerBackoffBean() {
		// TODO: Maybe there's a better way to instantiate and register this bean
		checkBeanFactoryInterfaces();
		AutowireCapableBeanFactory autowireCapableBeanFactory = (AutowireCapableBeanFactory) this.beanFactory;
		KafkaConsumerBackoffManager kafkaConsumerBackoffManager = autowireCapableBeanFactory.createBean(KafkaConsumerBackoffManager.class);
		ApplicationEventListenerInjector eventListenerInjector = autowireCapableBeanFactory.createBean(ApplicationEventListenerInjector.class);
		eventListenerInjector.addToApplicationListeners(kafkaConsumerBackoffManager);
		((SingletonBeanRegistry) this.beanFactory).registerSingleton(INTERNAL_KAFKA_CONSUMER_BACKOFF_BEAN_NAME, kafkaConsumerBackoffManager);
		return kafkaConsumerBackoffManager;
	}

	private void checkBeanFactoryInterfaces() {
		if (!AutowireCapableBeanFactory.class.isAssignableFrom(this.beanFactory.getClass()) || !SingletonBeanRegistry.class.isAssignableFrom(this.beanFactory.getClass())) {
			throw new IllegalStateException(String.format("Bean factory must be an instance of %s and %s. Provided type: %s",
					AutowireCapableBeanFactory.class.getSimpleName(), SingletonBeanRegistry.class.getSimpleName(),
					this.beanFactory.getClass().getSimpleName()));
		}
	}

	@SuppressWarnings("unchecked")
	private <T> T checkAndCast(Object obj, Class<T> clazz) {
		Assert.isAssignable(clazz, obj.getClass(),
				() -> String.format("The provided class %s is not assignable from %s", obj.getClass().getSimpleName(), clazz.getSimpleName()));
		return (T) obj;
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	private KafkaListenerEndpointRegistry getEndpointRegistryBean() {
		return this.beanFactory.getBean(KafkaListenerEndpointRegistry.class);
	}

	private final static class ApplicationEventListenerInjector {
		private final ConfigurableApplicationContext applicationContext;

		private ApplicationEventListenerInjector(ApplicationContext applicationContext) {
			if (!ConfigurableApplicationContext.class.isAssignableFrom(applicationContext.getClass())) {
				throw new IllegalArgumentException(String.format("ApplicationContext must be an instance of %s. Provided: %s",
						ConfigurableApplicationContext.class.getSimpleName(), applicationContext.getClass().getSimpleName()));
			}
			this.applicationContext = (ConfigurableApplicationContext) applicationContext;
		}

		void addToApplicationListeners(ApplicationListener<?> applicationListener) {
			this.applicationContext.addApplicationListener(applicationListener);
		}
	}

	static final class DeadLetterPublishingRecovererProvider {

		private final CharSequence KAFKA_DLT_HEADERS_PREFIX = "kafka_dlt";
		private final BinaryExceptionClassifier NO_CLASSIFIER = null;
		private final Map<Class<?>, KafkaOperations<?, ?>> templates;
		private final BinaryExceptionClassifier exceptionClassifier;
		private final DestinationTopicResolver destinationTopicResolver;

		private DeadLetterPublishingRecovererProvider(Map<Class<?>, KafkaOperations<?, ?>> templates, BinaryExceptionClassifier exceptionClassifier, DestinationTopicResolver destinationTopicResolver) {
			this.exceptionClassifier = exceptionClassifier;
			this.destinationTopicResolver = destinationTopicResolver;
			Assert.isTrue(templates != null && !templates.isEmpty(),
					() -> "At least one template is necessary to configure Retry Topic");
			this.templates = templates;
		}

		public DeadLetterPublishingRecoverer create() {
			DeadLetterPublishingRecoverer recoverer = this.templates.size() == 1
					? new DeadLetterPublishingRecoverer(this.templates.entrySet().iterator().next().getValue(), ((cr, e) -> this.resolveDestination(cr, e, this.destinationTopicResolver)))
					: new DeadLetterPublishingRecoverer(this.templates, (cr, e) -> resolveDestination(cr, e, this.destinationTopicResolver));

			recoverer.setHeadersFunction((consumerRecord, e) -> addBackoffHeader(consumerRecord, this.destinationTopicResolver));
			return recoverer;
		}

		private TopicPartition resolveDestination(ConsumerRecord<?, ?> cr, Exception e, DestinationTopicResolver destinationTopicResolver) {
			if (isBackoffException(e)) {
				throw (NestedRuntimeException) e; // Necessary to not commit the offset and seek to current again
			}

			// TODO: Handle the cases where the destination topics has less than the main topic number of partitions
			return this.exceptionClassifier == this.NO_CLASSIFIER
					|| this.exceptionClassifier.classify(e)
					? new TopicPartition(destinationTopicResolver.resolveDestinationFor(cr.topic()), cr.partition())
					: handleDltTopicPartition(cr, destinationTopicResolver);
		}

		private TopicPartition handleDltTopicPartition(ConsumerRecord<?, ?> cr, DestinationTopicResolver destinationTopicResolver) {
			return new TopicPartition(destinationTopicResolver.resolveDltDestinationFor(cr.topic()), cr.partition());
		}

		private boolean isBackoffException(Exception e) {
			return NestedRuntimeException.class.isAssignableFrom(e.getClass())
					&& ((NestedRuntimeException) e).contains(KafkaBackoffException.class);
		}

		private Headers addBackoffHeader(ConsumerRecord<?, ?> consumerRecord, DestinationTopicResolver destinationTopicResolver) {
			Headers headers = filterPreviousDltHeaders(consumerRecord.headers());
			headers.add(KafkaBackoffAwareMessageListenerAdapter.DEFAULT_HEADER_BACKOFF_TIMESTAMP, destinationTopicResolver.resolveDestinationNextExecutionTime(consumerRecord.topic()).getBytes());
			return headers;
		}

		// TODO: Integrate better with the DeadLetterPublisherRecoverer so that the headers end up better.
		private RecordHeaders filterPreviousDltHeaders(Headers headers) {
			return StreamSupport
					.stream(headers.spliterator(), false)
					.filter(header -> !new String(header.value()).contains(this.KAFKA_DLT_HEADERS_PREFIX))
					.reduce(new RecordHeaders(), ((recordHeaders, header) -> {
						recordHeaders.add(header);
						return recordHeaders;
					}), (a, b) -> a);
		}
	}
}
