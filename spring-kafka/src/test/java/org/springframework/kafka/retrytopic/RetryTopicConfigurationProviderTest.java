package org.springframework.kafka.retrytopic;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaOperations;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetryTopicConfigurationProviderTest {

	@Mock
	ListableBeanFactory beanFactory;

	String[] topics = {"topic1", "topic2"};

	Method annotatedMethod = getAnnotatedMethod("annotatedMethod");

	Method nonAnnotatedMethod = getAnnotatedMethod("nonAnnotatedMethod");

	@NotNull
	private Method getAnnotatedMethod(String methodName) {
		try {
			return  this.getClass().getDeclaredMethod(methodName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Mock
	Object bean;

	@Mock
	RetryableTopic annotation;

	@Mock
	KafkaOperations kafkaOperations;

	@Mock
	RetryTopicConfiguration retryTopicConfiguration;

	@Mock
	RetryTopicConfiguration retryTopicConfiguration2;

	@Test
	void shouldProvideFromAnnotation() {

		// setup
		doReturn(kafkaOperations).when(beanFactory).getBean(RetryTopicConfigUtils.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class);

		// when
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, annotatedMethod, bean);

		// then
		verify(this.beanFactory, times(0)).getBeansOfType(RetryTopicConfiguration.class);

	}

	@Test
	void shouldProvideFromBeanFactory() {

		// setup
		doReturn(Collections.singletonMap("retryTopicConfiguration", retryTopicConfiguration))
				.when(this.beanFactory).getBeansOfType(RetryTopicConfiguration.class);
		when(retryTopicConfiguration.hasConfigurationForTopics(topics)).thenReturn(true);

		// when
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, nonAnnotatedMethod, bean);

		// then
		verify(this.beanFactory, times(1)).getBeansOfType(RetryTopicConfiguration.class);
		assertEquals(retryTopicConfiguration, configuration);

	}

	@Test
	void shouldFindNone() {

		// setup
		doReturn(Collections.singletonMap("retryTopicConfiguration", retryTopicConfiguration))
				.when(this.beanFactory).getBeansOfType(RetryTopicConfiguration.class);
		when(retryTopicConfiguration.hasConfigurationForTopics(topics)).thenReturn(false);

		// when
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(beanFactory);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, nonAnnotatedMethod, bean);

		// then
		verify(this.beanFactory, times(1)).getBeansOfType(RetryTopicConfiguration.class);
		assertNull(configuration);

	}


	@Test
	void shouldNotConfigureIfBeanFactoryNull() {

		// when
		RetryTopicConfigurationProvider provider = new RetryTopicConfigurationProvider(null);
		RetryTopicConfiguration configuration = provider.findRetryConfigurationFor(topics, nonAnnotatedMethod, bean);

		// then
		assertNull(configuration);

	}

	@RetryableTopic
	public void annotatedMethod() {
		// NoOps
	}

	public void nonAnnotatedMethod() {
		// NoOps
	}
}