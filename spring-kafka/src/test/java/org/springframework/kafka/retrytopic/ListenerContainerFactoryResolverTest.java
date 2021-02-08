package org.springframework.kafka.retrytopic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ListenerContainerFactoryResolverTest {

	@Mock
	BeanFactory beanFactory;

	@Mock
	ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromKafkaListenerAnnotation;

	@Mock
	ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromRetryTopicConfiguration;

	@Mock
	ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromBeanName;

	@Mock
	ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromOtherBeanName;

	@Mock
	ConcurrentKafkaListenerContainerFactory<?, ?> factoryFromDefaultBeanName;

	private final static String factoryName = "testListenerContainerFactory";
	private final static String otherFactoryName = "otherTestListenerContainerFactory";

	@Test
	void shouldResolveWithKLAFactoryForMainEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, this.factoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromKafkaListenerAnnotation, resolvedFactory);
	}

	@Test
	void shouldResolveWithRTConfigurationFactoryForMainEndpointIfKLAAbsent() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, this.factoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		// then
		assertEquals(factoryFromRetryTopicConfiguration, resolvedFactory);
	}

	@Test
	void shouldResolveFromBeanNameForMainEndpoint() {

		// setup
		when(beanFactory.getBean(factoryName, ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, this.factoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldResolveFromDefaultBeanNameForMainEndpoint() {

		// setup
		when(beanFactory.getBean(RetryTopicConfigUtils.DEFAULT_LISTENER_FACTORY_BEAN_NAME,
				ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldFailIfNoneResolvedForMainEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		// when
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// then
		assertThrows(IllegalArgumentException.class,
				() -> listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration));
	}

	@Test
	void shouldResolveWithRetryTopicConfigurationFactoryForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, this.factoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromRetryTopicConfiguration, resolvedFactory);
	}

	@Test
	void shouldResolveFromBeanNameForRetryEndpoint() {

		// setup
		when(beanFactory.getBean(factoryName, ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(null, this.factoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory = listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldResolveWithKLAFactoryForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(factoryFromKafkaListenerAnnotation, configuration);

		// then
		assertEquals(factoryFromKafkaListenerAnnotation, resolvedFactory);
	}

	@Test
	void shouldResolveFromDefaultBeanNameForRetryEndpoint() {

		// setup
		when(beanFactory.getBean(RetryTopicConfigUtils.DEFAULT_LISTENER_FACTORY_BEAN_NAME,
				ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
	}

	@Test
	void shouldFailIfNoneResolvedForRetryEndpoint() {

		// setup
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		// when
		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, null);

		// then
		assertThrows(IllegalArgumentException.class,
				() -> listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration));
	}

	@Test
	void shouldGetFromCacheForMainEndpont() {

		// setup
		when(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration2);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
		assertEquals(factoryFromBeanName, resolvedFactory2);
		verify(beanFactory, times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldGetFromCacheForRetryEndpont() {

		// setup
		when(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForRetryEndpoint(null, configuration2);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
		assertEquals(factoryFromBeanName, resolvedFactory2);
		verify(beanFactory, times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldNotGetFromCacheForMainEndpont() {

		// setup
		when(beanFactory.getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromBeanName);
		when(beanFactory.getBean(otherFactoryName,
				ConcurrentKafkaListenerContainerFactory.class)).thenReturn(factoryFromOtherBeanName);
		ListenerContainerFactoryResolver listenerContainerFactoryResolver = new ListenerContainerFactoryResolver(beanFactory);

		ListenerContainerFactoryResolver.Configuration configuration =
				new ListenerContainerFactoryResolver.Configuration(null, factoryName);

		ListenerContainerFactoryResolver.Configuration configuration2 =
				new ListenerContainerFactoryResolver.Configuration(null, otherFactoryName);

		// when
		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration);

		ConcurrentKafkaListenerContainerFactory<?, ?> resolvedFactory2 =
				listenerContainerFactoryResolver.resolveFactoryForMainEndpoint(null, configuration2);

		// then
		assertEquals(factoryFromBeanName, resolvedFactory);
		assertEquals(factoryFromOtherBeanName, resolvedFactory2);
		verify(beanFactory, times(1)).getBean(factoryName,
				ConcurrentKafkaListenerContainerFactory.class);
		verify(beanFactory, times(1)).getBean(otherFactoryName,
				ConcurrentKafkaListenerContainerFactory.class);
	}

	@Test
	void shouldGetFromCacheWithSameConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// when
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertEquals(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration), factoryFromDefaultBeanName);
	}

	@Test
	void shouldGetFromCacheWithEqualConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);
		ListenerContainerFactoryResolver.Configuration configuration2 = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);

		// when
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertEquals(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration2), factoryFromDefaultBeanName);
	}

	@Test
	void shouldNotGetFromCacheWithDifferentConfiguration() {

		// setup
		ListenerContainerFactoryResolver.Cache cache = new ListenerContainerFactoryResolver.Cache();
		ListenerContainerFactoryResolver.Configuration configuration = new ListenerContainerFactoryResolver.Configuration(factoryFromRetryTopicConfiguration, factoryName);
		ListenerContainerFactoryResolver.Configuration configuration2 = new ListenerContainerFactoryResolver.Configuration(factoryFromOtherBeanName, factoryName);

		// when
		cache.addIfAbsent(factoryFromKafkaListenerAnnotation, configuration, factoryFromDefaultBeanName);

		// then
		assertNull(cache.fromCache(factoryFromKafkaListenerAnnotation, configuration2));
	}
}