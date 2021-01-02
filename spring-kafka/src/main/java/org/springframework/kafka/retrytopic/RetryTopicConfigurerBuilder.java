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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.BinaryExceptionClassifierBuilder;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicContainer;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicContainerFactory;
import org.springframework.kafka.support.AllowDenyCollectionManager;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.NoBackOffPolicy;
import org.springframework.retry.backoff.SleepingBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;
import org.springframework.util.Assert;


/**
 *
 * Builder class to instantiate the RetryTopicConfigurer.
 *
 * Refer to {@link RetryTopicConfigurer} and {@link org.springframework.kafka.annotation.RetryableTopic} for more details.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 *
 * @see RetryTopicConfigurer
 * @see org.springframework.kafka.annotation.RetryableTopic
 */
public class RetryTopicConfigurerBuilder {
	private int maxAttempts = BackOffValuesGenerator.NOT_SET;
	private BackOffPolicy backOffPolicy;
	private RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod;
	private List<String> includeTopicNames = new ArrayList<>();
	private List<String> excludeTopicNames = new ArrayList<>();
	private String retryTopicSuffix;
	private String dltSuffix;
	private BeanFactory beanFactory;
	private KafkaTopicAutoCreationManager kafkaTopicAutoCreationManager = new KafkaTopicAutoCreationManager();
	private ConcurrentKafkaListenerContainerFactory<?, ?> listenerContainerFactory;
	private String listenerContainerFactoryName;
	private BinaryExceptionClassifierBuilder classifierBuilder;

	/* ---------------- Configure Dlt Bean and Method -------------- */
	public RetryTopicConfigurerBuilder dltHandlerMethod(Class<?> clazz, String methodName) {
		this.dltHandlerMethod = RetryTopicConfigurer.createHandlerMethodWith(clazz, methodName);
		return this;
	}

	RetryTopicConfigurerBuilder dltHandlerMethod(RetryTopicConfigurer.EndpointHandlerMethod endpointHandlerMethod) {
		this.dltHandlerMethod = endpointHandlerMethod;
		return this;
	}

	/* ---------------- Configure Topic GateKeeper -------------- */
	public RetryTopicConfigurerBuilder includeTopics(List<String> topicNames) {
		this.includeTopicNames.addAll(topicNames);
		return this;
	}

	public RetryTopicConfigurerBuilder excludeTopics(List<String> topicNames) {
		this.excludeTopicNames.addAll(topicNames);
		return this;
	}

	public RetryTopicConfigurerBuilder includeTopic(String topicName) {
		this.includeTopicNames.add(topicName);
		return this;
	}

	public RetryTopicConfigurerBuilder excludeTopic(String topicName) {
		this.excludeTopicNames.add(topicName);
		return this;
	}

	/* ---------------- Configure Topic Suffixes -------------- */

	public RetryTopicConfigurerBuilder retryTopicSuffix(String suffix) {
		this.retryTopicSuffix = suffix;
		return this;
	}

	public RetryTopicConfigurerBuilder dltSuffix(String suffix) {
		this.dltSuffix = suffix;
		return this;
	}

	/* ---------------- Configure BackOff -------------- */

	public RetryTopicConfigurerBuilder maxAttempts(int maxAttempts) {
		Assert.isTrue(maxAttempts > 0, "Number of attempts should be positive");
		Assert.isTrue(this.maxAttempts == BackOffValuesGenerator.NOT_SET, "You have already set the number of attempts");
		this.maxAttempts = maxAttempts;
		return this;
	}

	public RetryTopicConfigurerBuilder exponentialBackoff(long initialInterval, double multiplier, long maxInterval) {
		return exponentialBackoff(initialInterval, multiplier, maxInterval, false);
	}

	public RetryTopicConfigurerBuilder exponentialBackoff(long initialInterval, double multiplier, long maxInterval,
														boolean withRandom) {
		Assert.isNull(this.backOffPolicy, "You have already selected backoff policy");
		Assert.isTrue(initialInterval >= 1, "Initial interval should be >= 1");
		Assert.isTrue(multiplier > 1, "Multiplier should be > 1");
		Assert.isTrue(maxInterval > initialInterval, "Max interval should be > than initial interval");
		ExponentialBackOffPolicy policy = withRandom ? new ExponentialRandomBackOffPolicy()
				: new ExponentialBackOffPolicy();
		policy.setInitialInterval(initialInterval);
		policy.setMultiplier(multiplier);
		policy.setMaxInterval(maxInterval);
		this.backOffPolicy = policy;
		return this;
	}

	public RetryTopicConfigurerBuilder fixedBackoff(long interval) {
		Assert.isNull(this.backOffPolicy, "You have already selected backoff policy");
		Assert.isTrue(interval >= 1, "Interval should be >= 1");
		FixedBackOffPolicy policy = new FixedBackOffPolicy();
		policy.setBackOffPeriod(interval);
		this.backOffPolicy = policy;
		return this;
	}

	public RetryTopicConfigurerBuilder uniformRandomBackoff(long minInterval, long maxInterval) {
		Assert.isNull(this.backOffPolicy, "You have already selected backoff policy");
		Assert.isTrue(minInterval >= 1, "Min interval should be >= 1");
		Assert.isTrue(maxInterval >= 1, "Max interval should be >= 1");
		Assert.isTrue(maxInterval > minInterval, "Max interval should be > than min interval");
		UniformRandomBackOffPolicy policy = new UniformRandomBackOffPolicy();
		policy.setMinBackOffPeriod(minInterval);
		policy.setMaxBackOffPeriod(maxInterval);
		this.backOffPolicy = policy;
		return this;
	}

	public RetryTopicConfigurerBuilder noBackoff() {
		Assert.isNull(this.backOffPolicy, "You have already selected backoff policy");
		this.backOffPolicy = new NoBackOffPolicy();
		return this;
	}

	public RetryTopicConfigurerBuilder customBackoff(SleepingBackOffPolicy<?> backOffPolicy) {
		Assert.isNull(this.backOffPolicy, "You have already selected backoff policy");
		Assert.notNull(backOffPolicy, "You should provide non null custom policy");
		this.backOffPolicy = backOffPolicy;
		return this;
	}

	public RetryTopicConfigurerBuilder fixedBackoff(int interval) {
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(interval);
		this.backOffPolicy = backOffPolicy;
		return this;
	}

	/* ---------------- Configure Topics Auto Creation -------------- */

	public RetryTopicConfigurerBuilder doNotAutoCreateTopics() {
		this.kafkaTopicAutoCreationManager = new KafkaTopicAutoCreationManager(false);
		return this;
	}

	public RetryTopicConfigurerBuilder autoCreateTopicsWith(int numPartitions, short replicationFactor) {
		this.kafkaTopicAutoCreationManager = new KafkaTopicAutoCreationManager(true, numPartitions, replicationFactor);
		return this;
	}

	public RetryTopicConfigurerBuilder autoCreateTopics(boolean shouldCreate, int numPartitions, short replicationFactor) {
		this.kafkaTopicAutoCreationManager = new KafkaTopicAutoCreationManager(shouldCreate, numPartitions, replicationFactor);
		return this;
	}

	/* ---------------- Configure Exception Classifier -------------- */

	public RetryTopicConfigurerBuilder retryOn(Class<? extends Throwable> throwable) {
		classifierBuilder().retryOn(throwable);
		return this;
	}

	public RetryTopicConfigurerBuilder notRetryOn(Class<? extends Throwable> throwable) {
		classifierBuilder().notRetryOn(throwable);
		return this;
	}

	public RetryTopicConfigurerBuilder retryOn(List<Class<? extends Throwable>> throwables) {
		throwables
				.stream()
				.forEach(throwable -> classifierBuilder().retryOn(throwable));
		return this;
	}

	public RetryTopicConfigurerBuilder notRetryOn(List<Class<? extends Throwable>> throwables) {
		throwables
				.stream()
				.forEach(throwable -> classifierBuilder().notRetryOn(throwable));
		return this;
	}

	public RetryTopicConfigurerBuilder traversingCauses() {
		classifierBuilder().traversingCauses();
		return this;
	}

	RetryTopicConfigurerBuilder traversingCauses(boolean traversing) {
		if (traversing) {
			classifierBuilder().traversingCauses();
		}
		return this;
	}

	private BinaryExceptionClassifierBuilder classifierBuilder() {
		if (this.classifierBuilder == null) {
			this.classifierBuilder = new BinaryExceptionClassifierBuilder();
		}
		return this.classifierBuilder;
	}

	/* ---------------- Configure KafkaListenerContainerFactory -------------- */
	public RetryTopicConfigurerBuilder listenerFactory(ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
		this.listenerContainerFactory = factory;
		return this;
	}

	public RetryTopicConfigurerBuilder listenerFactory(String factoryBeanName) {
		this.listenerContainerFactoryName = factoryBeanName;
		return this;
	}

	public RetryTopicConfigurer create(KafkaOperations<?, ?> sendToTopicKafkaTemplate) {
		return create(Collections.singletonMap(Object.class, sendToTopicKafkaTemplate));
	}

	public RetryTopicConfigurer create(Map<Class<?>, KafkaOperations<?, ?>> sendToTopicKafkaTemplate) {
		DestinationTopicContainerFactory destinationTopicContainerFactory = new DestinationTopicContainerFactory(this.retryTopicSuffix, this.dltSuffix, this.maxAttempts, this.backOffPolicy);
		DestinationTopicContainer destinationTopicContainer = destinationTopicContainerFactory.createDestinationTopicContainer();
		ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer = new ListenerContainerFactoryConfigurer(sendToTopicKafkaTemplate, buildClassifier(), destinationTopicContainer);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(this.listenerContainerFactory, this.listenerContainerFactoryName);
		AllowDenyCollectionManager<String> topicManager = AllowDenyCollectionManager.createManagerFor(this.includeTopicNames, this.excludeTopicNames);

		return new RetryTopicConfigurer(
				this.dltHandlerMethod,
				topicManager,
				destinationTopicContainer,
				listenerContainerFactoryResolver,
				listenerContainerFactoryConfigurer,
				this.kafkaTopicAutoCreationManager);
	}

	private BinaryExceptionClassifier buildClassifier() {
		return this.classifierBuilder != null
				? this.classifierBuilder.build()
				: null;
	}
}
