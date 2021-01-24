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

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Class that resolves a {@link ConcurrentKafkaListenerContainerFactory} to be used by the RetryTopicConfigurer.
 *
 * As the factory can be provided externally, it's important to notice that the same factory instance must NOT used by both retrying
 * and non retrying topics because the retry topic configuration will interfere with the non retrying topics.
 *
 * It's ok however to share the same provided factory between retryable topics.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 *
 * @see ListenerContainerFactoryConfigurer
 *
 */
public class ListenerContainerFactoryResolver {

	private final static String DEFAULT_LISTENER_FACTORY_BEAN_NAME = "retryTopicListenerContainerFactory";
	private final static ConcurrentKafkaListenerContainerFactory<?, ?> NO_CANDIDATE = null;
	private final BeanFactory beanFactory;
	private final List<FactoryResolver> mainEndpointResolvers;
	private final List<FactoryResolver> retryEndpointResolvers;

	ListenerContainerFactoryResolver(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;

		this.mainEndpointResolvers = Arrays.asList(
				(candidate, configuration) -> candidate,
				(candidate, configuration) -> configuration.providedListenerContainerFactory,
				(candidate, configuration) -> getFactoryFromBeanName(configuration.listenerContainerFactoryName),
				(candidate, configuration) -> getFactoryFromBeanName(DEFAULT_LISTENER_FACTORY_BEAN_NAME));

		this.retryEndpointResolvers = Arrays.asList(
				(candidate, configuration) -> configuration.providedListenerContainerFactory,
				(candidate, configuration) -> getFactoryFromBeanName(configuration.listenerContainerFactoryName),
				(candidate, configuration) -> candidate,
				(candidate, configuration) -> getFactoryFromBeanName(DEFAULT_LISTENER_FACTORY_BEAN_NAME));
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForMainEndpoint(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
		return resolveFactory(this.mainEndpointResolvers, factoryFromKafkaListenerAnnotation, config);
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForRetryEndpoint(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
		return resolveFactory(this.retryEndpointResolvers, factoryFromKafkaListenerAnnotation, config);
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(List<FactoryResolver> factoryResolvers, KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
		ConcurrentKafkaListenerContainerFactory<?, ?> providedContainerFactoryCandidate = getProvidedCandidate(factoryFromKafkaListenerAnnotation);
		ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory = factoryResolvers
				.stream()
				.map(resolver -> Optional.ofNullable(resolver.resolveFactory(providedContainerFactoryCandidate, config)))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("Could not resolve a viable ConcurrentKafkaListenerContainerFactory to configure the retry topic. Try creating a bean with name " + DEFAULT_LISTENER_FACTORY_BEAN_NAME));
		return containerFactory;
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> getProvidedCandidate(KafkaListenerContainerFactory<?> fromKafkaListenerAnnotationFactory) {
		return fromKafkaListenerAnnotationFactory != NO_CANDIDATE && ConcurrentKafkaListenerContainerFactory.class.isAssignableFrom(fromKafkaListenerAnnotationFactory.getClass())
				? (ConcurrentKafkaListenerContainerFactory<?, ?>) fromKafkaListenerAnnotationFactory
				: NO_CANDIDATE;
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> getFactoryFromBeanName(String factoryBeanName) {
		return StringUtils.hasText(factoryBeanName)
				? this.beanFactory.getBean(factoryBeanName, ConcurrentKafkaListenerContainerFactory.class)
				: NO_CANDIDATE;
	}

	private interface FactoryResolver {
		ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(ConcurrentKafkaListenerContainerFactory<?, ?> candidate, Configuration configuration);
	}

	static class Configuration {
		// ListenerContainerFactory
		private final ConcurrentKafkaListenerContainerFactory<?, ?> providedListenerContainerFactory;
		private final String listenerContainerFactoryName;

		Configuration(ConcurrentKafkaListenerContainerFactory<?, ?> providedListenerContainerFactory, String listenerContainerFactoryName) {
			this.providedListenerContainerFactory = providedListenerContainerFactory;
			this.listenerContainerFactoryName = listenerContainerFactoryName;
		}
	}
}
