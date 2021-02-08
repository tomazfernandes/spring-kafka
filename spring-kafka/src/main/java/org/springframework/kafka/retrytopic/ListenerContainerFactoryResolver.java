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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

	private final static ConcurrentKafkaListenerContainerFactory<?, ?> NO_CANDIDATE = null;
	private final BeanFactory beanFactory;
	private final List<FactoryResolver> mainEndpointResolvers;
	private final List<FactoryResolver> retryEndpointResolvers;
	private final Cache mainEndpointCache;
	private final Cache retryEndpointCache;


	ListenerContainerFactoryResolver(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
		this.mainEndpointCache = new Cache();
		this.retryEndpointCache = new Cache();

		this.mainEndpointResolvers = Arrays.asList(
				(fromKafkaListenerAnnotation, configuration) -> mainEndpointCache.fromCache(fromKafkaListenerAnnotation, configuration),
				(fromKafkaListenerAnnotation, configuration) -> fromKafkaListenerAnnotation,
				(fromKLAnnotation, configuration) -> configuration.factoryFromRetryTopicConfiguration,
				(fromKLAnnotation, configuration) -> fromBeanName(configuration.listenerContainerFactoryName),
				(fromKLAnnotation, configuration) -> fromBeanName(RetryTopicConfigUtils.DEFAULT_LISTENER_FACTORY_BEAN_NAME));

		this.retryEndpointResolvers = Arrays.asList(
				(fromKafkaListenerAnnotation, configuration) -> retryEndpointCache.fromCache(fromKafkaListenerAnnotation, configuration),
				(fromKLAnnotation, configuration) -> configuration.factoryFromRetryTopicConfiguration,
				(fromKLAnnotation, configuration) -> fromBeanName(configuration.listenerContainerFactoryName),
				(fromKLAnnotation, configuration) -> fromKLAnnotation,
				(fromKLAnnotation, configuration) -> fromBeanName(RetryTopicConfigUtils.DEFAULT_LISTENER_FACTORY_BEAN_NAME));
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForMainEndpoint(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = resolveFactory(this.mainEndpointResolvers, factoryFromKafkaListenerAnnotation, config);
		return mainEndpointCache.addIfAbsent(factoryFromKafkaListenerAnnotation, config, resolvedFactory);
	}

	ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactoryForRetryEndpoint(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = resolveFactory(this.retryEndpointResolvers, factoryFromKafkaListenerAnnotation, config);
		return retryEndpointCache.addIfAbsent(factoryFromKafkaListenerAnnotation, config, resolvedFactory);
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(List<FactoryResolver> factoryResolvers, KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
		ConcurrentKafkaListenerContainerFactory<?, ?> verifiedFactoryFromKafkaListenerAnnotation = verifyClass(factoryFromKafkaListenerAnnotation);
		ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory = factoryResolvers
				.stream()
				.map(resolver -> Optional.ofNullable(resolver.resolveFactory(verifiedFactoryFromKafkaListenerAnnotation, config)))
				.filter(Optional::isPresent)
				.map(Optional::get)
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Could not resolve a viable ConcurrentKafkaListenerContainerFactory to configure the retry topic. Try creating a bean with name " + RetryTopicConfigUtils.DEFAULT_LISTENER_FACTORY_BEAN_NAME));
		return containerFactory;
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> verifyClass(KafkaListenerContainerFactory<?> fromKafkaListenerAnnotationFactory) {
		return fromKafkaListenerAnnotationFactory != NO_CANDIDATE && ConcurrentKafkaListenerContainerFactory.class.isAssignableFrom(fromKafkaListenerAnnotationFactory.getClass())
				? (ConcurrentKafkaListenerContainerFactory<?, ?>) fromKafkaListenerAnnotationFactory
				: NO_CANDIDATE;
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> fromBeanName(String factoryBeanName) {
		return StringUtils.hasText(factoryBeanName)
				? this.beanFactory.getBean(factoryBeanName, ConcurrentKafkaListenerContainerFactory.class)
				: NO_CANDIDATE;
	}

	private interface FactoryResolver {
		ConcurrentKafkaListenerContainerFactory<?, ?> resolveFactory(ConcurrentKafkaListenerContainerFactory<?, ?> candidate, Configuration configuration);
	}

	static class Configuration {
		private final ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromRetryTopicConfiguration;
		private final String listenerContainerFactoryName;

		Configuration(ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromRetryTopicConfiguration, String listenerContainerFactoryName) {
			this.factoryFromRetryTopicConfiguration = factoryFromRetryTopicConfiguration;
			this.listenerContainerFactoryName = listenerContainerFactoryName;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Configuration that = (Configuration) o;
			return Objects.equals(factoryFromRetryTopicConfiguration, that.factoryFromRetryTopicConfiguration) && Objects.equals(listenerContainerFactoryName, that.listenerContainerFactoryName);
		}

		@Override
		public int hashCode() {
			return Objects.hash(factoryFromRetryTopicConfiguration, listenerContainerFactoryName);
		}
	}

	static class Cache {

		private final Map<Key, ConcurrentKafkaListenerContainerFactory<?, ?>> cache;

		Cache() {
			this.cache = new HashMap<>();
		}

		ConcurrentKafkaListenerContainerFactory<?, ?> addIfAbsent(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation,
																  Configuration config,
																  ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory) {
			synchronized (cache) {
				Key key = cacheKey(factoryFromKafkaListenerAnnotation, config);
				if (!cache.containsKey(key)) {
					cache.put(key, resolvedFactory);
				}
				return resolvedFactory;
			}
		}

		ConcurrentKafkaListenerContainerFactory<?, ?> fromCache(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
			synchronized (cache) {
				return cache.get(cacheKey(factoryFromKafkaListenerAnnotation, config));
			}
		}

		private Key cacheKey(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
			return new Key(factoryFromKafkaListenerAnnotation, config);
		}

		class Key {
			private final KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation;
			private final Configuration config;

			Key(KafkaListenerContainerFactory<?> factoryFromKafkaListenerAnnotation, Configuration config) {
				this.factoryFromKafkaListenerAnnotation = factoryFromKafkaListenerAnnotation;
				this.config = config;
			}

			@Override
			public boolean equals(Object o) {
				if (this == o) return true;
				if (o == null || getClass() != o.getClass()) return false;
				Key key = (Key) o;
				return Objects.equals(factoryFromKafkaListenerAnnotation, key.factoryFromKafkaListenerAnnotation) && Objects.equals(config, key.config);
			}

			@Override
			public int hashCode() {
				return Objects.hash(factoryFromKafkaListenerAnnotation, config);
			}
		}
	}
}
