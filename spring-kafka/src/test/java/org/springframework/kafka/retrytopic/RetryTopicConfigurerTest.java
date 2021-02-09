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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.times;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicProcessor;
import org.springframework.kafka.support.Suffixer;

/**
 * @author Tomaz Fernandes
 * @since 2.7.0
 */
@ExtendWith(MockitoExtension.class)
class RetryTopicConfigurerTest {

	@Mock
	DestinationTopicProcessor destinationTopicProcessor;

	@Mock
	ListenerContainerFactoryResolver containerFactoryResolver;

	@Mock
	ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer;

	@Mock
	BeanFactory beanFactory;

	@Mock
	DefaultListableBeanFactory defaultListableBeanFactory;

	@Mock
	RetryTopicConfigurer.EndpointProcessor endpointProcessor;

	@Mock
	MethodKafkaListenerEndpoint<?, ?> mainEndpoint;

	@Mock
	MultiMethodKafkaListenerEndpoint<?, ?> multiMethodEndpoint;

	@Mock
	RetryTopicConfiguration configuration;

	@Mock
	DestinationTopic.Properties mainDestinationProperties;

	@Mock
	DestinationTopic.Properties firstRetryDestinationProperties;

	@Mock
	DestinationTopic.Properties secondRetryDestinationProperties;

	@Mock
	DestinationTopic.Properties dltDestinationProperties;

	@Mock
	DeadLetterPublishingRecovererFactory.Configuration deadLetterConfiguration;

	@Mock
	ListenerContainerFactoryResolver.Configuration factoryResolverConfig;

	@Mock
	ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory;

	@Captor
	ArgumentCaptor<RetryTopicConfigurer.EndpointProcessingCustomizerHolder> mainCustomizerHolderCaptor;

	@Captor
	ArgumentCaptor<RetryTopicConfigurer.EndpointProcessingCustomizerHolder> retryCustomizerHolderCaptor;

	@Captor
	ArgumentCaptor<Consumer<DestinationTopic.Properties>> destinationPropertiesProcessorCaptor;

	@Captor
	ArgumentCaptor<DestinationTopicProcessor.Context> contextCaptor;

	@Mock
	RetryTopicConfigurer.EndpointHandlerMethod endpointHandlerMethod;

	@Mock
	Consumer<DestinationTopic.Properties> destinationPropertiesConsumer;

	@Mock
	MethodKafkaListenerEndpoint<?, ?> retryEndpoint1;

	@Mock
	MethodKafkaListenerEndpoint<?, ?> retryEndpoint2;

	@Mock
	MethodKafkaListenerEndpoint<?, ?> dltEndpoint;

	List<String> topics = Arrays.asList("topic1", "topic2");

	private final String noOpsMethodName = "noOpsMethod";
	private final String noOpsDltMethodName = "noOpsDltMethod";
	Method endpointMethod = getMethod(noOpsMethodName);
	Method noOpsDltMethod = getMethod(noOpsDltMethodName);

	private Object bean = new Object();

	@Mock
	private ConsumerRecord<?, ?> consumerRecordMessage;

	private Object objectMessage = new Object();

	private Method getMethod(String methodName)  {
		try {
			return this.getClass().getMethod(methodName);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void shouldThrowIfMultiMethodEndpoint() {

		// setup
		RetryTopicConfigurer configurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver,
				listenerContainerFactoryConfigurer, beanFactory);

		// given - then
		assertThrows(IllegalArgumentException.class,
				() -> configurer.processMainAndRetryListeners(endpointProcessor, multiMethodEndpoint, configuration));
	}

	@Test
	void shouldConfigureRetryEndpoints() {

		// setup

		List<DestinationTopic.Properties> destinationPropertiesList =
				Arrays.asList(mainDestinationProperties, firstRetryDestinationProperties,
						secondRetryDestinationProperties, dltDestinationProperties);


		List<MethodKafkaListenerEndpoint<?, ?>> endpoints = Arrays.asList(mainEndpoint, retryEndpoint1, retryEndpoint2, dltEndpoint);

		IntStream.range(1, endpoints.size())
				.forEach(index -> {
			MethodKafkaListenerEndpoint<?, ?> endpoint = endpoints.get(index);
			given(endpoint.getId()).willReturn("testId");
			given(endpoint.getGroup()).willReturn("testGroup");
			given(endpoint.getGroupId()).willReturn("testGroupId");
			given(endpoint.getClientIdPrefix()).willReturn("testClientPrefix");
			given(endpoint.getTopics()).willReturn(topics);
		});

		given(configuration.getDestinationTopicProperties()).willReturn(destinationPropertiesList);
		given(mainEndpoint.getBean()).willReturn(bean);
		given(mainEndpoint.getMethod()).willReturn(endpointMethod);
		given(configuration.getDltHandlerMethod()).willReturn(endpointHandlerMethod);

		given(configuration.forContainerFactoryResolver()).willReturn(factoryResolverConfig);
		given(configuration.forDeadLetterFactory()).willReturn(deadLetterConfiguration);
		willReturn(containerFactory).given(listenerContainerFactoryConfigurer).configure(containerFactory, deadLetterConfiguration);
		willReturn(containerFactory).given(containerFactoryResolver).resolveFactoryForMainEndpoint(any(KafkaListenerContainerFactory.class),
				eq(factoryResolverConfig));
		given(firstRetryDestinationProperties.suffix()).willReturn("-retry");
		given(secondRetryDestinationProperties.suffix()).willReturn("-retry");
		given(dltDestinationProperties.suffix()).willReturn("-dlt");

		RetryTopicConfigurer configurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver,
				listenerContainerFactoryConfigurer, beanFactory);

		// given
		configurer.processMainAndRetryListeners(endpointProcessor, mainEndpoint, configuration);

		// then
		then(endpointProcessor).should(times(1))
				.accept(eq(mainEndpoint), mainCustomizerHolderCaptor.capture());
		RetryTopicConfigurer.EndpointProcessingCustomizerHolder mainHolder = mainCustomizerHolderCaptor.getValue();

		MethodKafkaListenerEndpoint<?, ?> processedEndpoint = mainHolder.getEndpointCustomizer().apply(mainEndpoint);
		KafkaListenerContainerFactory<?> customizedFactory = mainHolder.getFactoryCustomizer().apply(containerFactory);

		then(destinationTopicProcessor).should(times(1))
				.processDestinationProperties(destinationPropertiesProcessorCaptor.capture(), contextCaptor.capture());

		Consumer<DestinationTopic.Properties> destinationPropertiesConsumer = destinationPropertiesProcessorCaptor.getValue();
		destinationPropertiesConsumer.accept(firstRetryDestinationProperties);
		destinationPropertiesConsumer.accept(secondRetryDestinationProperties);
		destinationPropertiesConsumer.accept(dltDestinationProperties);

		then(endpointProcessor).should(times(4))
				.accept(any(MethodKafkaListenerEndpoint.class), retryCustomizerHolderCaptor.capture());
		List<RetryTopicConfigurer.EndpointProcessingCustomizerHolder> allRetryHolders = retryCustomizerHolderCaptor.getAllValues();

		DestinationTopicProcessor.Context context = contextCaptor.getValue();

		IntStream.range(1, allRetryHolders.size())
				.boxed()
				.map(index -> {
					allRetryHolders.get(index).getEndpointCustomizer().apply(endpoints.get(index));
					return index;
				})
				.forEach(index -> thenEndpointCustomizing(endpoints.get(index), destinationPropertiesList.get(index), context));

		KafkaListenerContainerFactory<?> customizedRetryFactory = allRetryHolders.get(1).getFactoryCustomizer().apply(containerFactory);

	}

	private void thenEndpointCustomizing(MethodKafkaListenerEndpoint<?, ?> endpoint, DestinationTopic.Properties properties, DestinationTopicProcessor.Context context) {
		Suffixer suffixer = new Suffixer(properties.suffix());
		then(endpoint).should(times(1)).setId(suffixer.maybeAddTo(endpoint.getId()));
		then(endpoint).should(times(1)).setGroupId(suffixer.maybeAddTo(endpoint.getGroupId()));
		then(endpoint).should(times(1)).setClientIdPrefix(suffixer.maybeAddTo(endpoint.getClientIdPrefix()));
		then(endpoint).should(times(1)).setGroup(suffixer.maybeAddTo(endpoint.getGroup()));
		endpoint
				.getTopics()
				.stream()
				.forEach(topic ->
						then(destinationTopicProcessor).should(times(1))
								.registerDestinationTopic(topic, suffixer.maybeAddTo(topic), properties, context)
						);
	}


	public void noOpsMethod() {
		// noOps
	}

	public void noOpsDltMethod() {
		// noOps
	}

	@Test
	void shouldGetBeanFromContainer() {

		// setup
		NoOpsClass noOps = new NoOpsClass();
		willReturn(noOps).given(beanFactory).getBean(NoOpsClass.class);
		RetryTopicConfigurer.EndpointHandlerMethod handlerMethod =
				RetryTopicConfigurer.createHandlerMethodWith(NoOpsClass.class, noOpsMethodName);

		// given
		Object resolvedBean = handlerMethod.resolveBean(this.beanFactory);

		// then
		assertEquals(noOps, resolvedBean);

	}

	@Test
	void shouldInstantiateIfNotInContainer() {

		// setup
		String beanName = NoOpsClass.class.getSimpleName() + "-handlerMethod";
		given(defaultListableBeanFactory.getBean(beanName)).willReturn(new NoOpsClass());
		willThrow(NoSuchBeanDefinitionException.class).given(defaultListableBeanFactory).getBean(NoOpsClass.class);
		RetryTopicConfigurer.EndpointHandlerMethod handlerMethod =
				RetryTopicConfigurer.createHandlerMethodWith(NoOpsClass.class, noOpsMethodName);

		// given
		Object resolvedBean = handlerMethod.resolveBean(this.defaultListableBeanFactory);

		// then
		then(defaultListableBeanFactory).should()
				.registerBeanDefinition(eq(beanName), any(RootBeanDefinition.class));
		assertTrue(NoOpsClass.class.isAssignableFrom(resolvedBean.getClass()));

	}

	@Test
	void shouldLogConsumerRecordMessage() {
		RetryTopicConfigurer.LoggingDltListenerHandlerMethod method =
				new RetryTopicConfigurer.LoggingDltListenerHandlerMethod();
		method.logMessage(consumerRecordMessage);
		then(consumerRecordMessage).should(times(0)).topic();
	}

	@Test
	void shouldNotLogObjectMessage() {
		RetryTopicConfigurer.LoggingDltListenerHandlerMethod method =
				new RetryTopicConfigurer.LoggingDltListenerHandlerMethod();
		method.logMessage(objectMessage);
	}

	static class NoOpsClass {
		void noOpsMethod() { };
	}
}
