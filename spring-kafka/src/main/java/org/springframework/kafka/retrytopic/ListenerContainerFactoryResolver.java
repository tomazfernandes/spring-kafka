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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

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
	private final ConcurrentKafkaListenerContainerFactory<?, ?> providedFromConstructorListenerContainerFactory;
	private BeanFactory beanFactory;
	private ConcurrentKafkaListenerContainerFactory<?, ?> configuredListenerContainerFactory;
	private final List<FactoryResolver> mainEndpointResolvers;
	private final List<FactoryResolver> retryEndpointResolvers;

	ListenerContainerFactoryResolver(ConcurrentKafkaListenerContainerFactory<?, ?> providedFromConstructorListenerContainerFactory,
									String listenerContainerFactoryNameFromAnnotation) {
		this.providedFromConstructorListenerContainerFactory = providedFromConstructorListenerContainerFactory;

		this.mainEndpointResolvers = Arrays.asList(
				candidate -> candidate,
				candidate -> this.configuredListenerContainerFactory,
				candidate -> this.providedFromConstructorListenerContainerFactory,
				candidate -> getFactoryFromBeanName(listenerContainerFactoryNameFromAnnotation),
				candidate -> getFactoryFromBeanName(DEFAULT_LISTENER_FACTORY_BEAN_NAME));

		this.retryEndpointResolvers = Arrays.asList(
				candidate -> this.configuredListenerContainerFactory,
				candidate -> this.providedFromConstructorListenerContainerFactory,
				candidate -> getFactoryFromBeanName(listenerContainerFactoryNameFromAnnotation),
				candidate -> candidate,
				candidate -> getFactoryFromBeanName(DEFAULT_LISTENER_FACTORY_BEAN_NAME));

	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForMainEndpoint(KafkaListenerContainerFactory<?> fromKafkaListenerAnnotationFactory) {
		return resolveFactory(this.mainEndpointResolvers, fromKafkaListenerAnnotationFactory);
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForRetryEndpoint(KafkaListenerContainerFactory<?> fromKafkaListenerAnnotationFactory) {
		return resolveFactory(this.retryEndpointResolvers, fromKafkaListenerAnnotationFactory);
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(List<FactoryResolver> factoryResolvers, KafkaListenerContainerFactory<?> fromKafkaListenerAnnotationFactory) {
		Assert.notNull(this.beanFactory, () -> "BeanFactory not set!");
		ConcurrentKafkaListenerContainerFactory<?, ?> providedContainerFactoryCandidate = getProvidedCandidate(fromKafkaListenerAnnotationFactory);
		ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory = factoryResolvers
				.stream()
				.map(resolver -> Optional.ofNullable(resolver.resolveFactory(providedContainerFactoryCandidate)))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("Could not resolve a viable ConcurrentKafkaListenerContainerFactory to configure the retry topic. Try creating a bean with name " + DEFAULT_LISTENER_FACTORY_BEAN_NAME));
		if (containerFactory != null && this.configuredListenerContainerFactory == null) {
			this.configuredListenerContainerFactory = containerFactory;
		}
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

	public void setBeanFactory(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}

	private interface FactoryResolver {
		ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(ConcurrentKafkaListenerContainerFactory<?, ?> candidate);
	}
}
