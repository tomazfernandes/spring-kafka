package org.springframework.kafka.retrytopic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
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
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicProcessor;
import org.springframework.kafka.support.Suffixer;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
	ConcurrentKafkaListenerContainerFactory containerFactory;

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
	MethodKafkaListenerEndpoint retryEndpoint1;

	@Mock
	MethodKafkaListenerEndpoint retryEndpoint2;

	@Mock
	MethodKafkaListenerEndpoint dltEndpoint;

	List<String> topics = Arrays.asList("topic1", "topic2");

	private final String noOpsMethodName = "noOpsMethod";
	private final String noOpsDltMethodName = "noOpsDltMethod";
	Method endpointMethod = getMethod(noOpsMethodName);
	Method noOpsDltMethod = getMethod(noOpsDltMethodName);

	private Object bean = new Object();

	@Mock
	private ConsumerRecord consumerRecordMessage;

	private Object objectMessage = new Object();

	@NotNull
	private Method getMethod(String methodName)  {
		try {
			return this.getClass().getMethod(methodName);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void shouldThrowIfMultiMethodEndpoint() {

		// setup
		RetryTopicConfigurer configurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver,
				listenerContainerFactoryConfigurer, beanFactory);

		// when - then
		assertThrows(IllegalArgumentException.class,
				() -> configurer.processMainAndRetryListeners(endpointProcessor, multiMethodEndpoint, configuration));
	}

	@Test
	void shouldConfigure() {

		List<DestinationTopic.Properties> destinationPropertiesList =
				Arrays.asList(mainDestinationProperties, firstRetryDestinationProperties,
						secondRetryDestinationProperties, dltDestinationProperties);

		// setup
		when(configuration.getDestinationTopicProperties()).thenReturn(destinationPropertiesList);
		when(mainEndpoint.getBean()).thenReturn(bean);
		when(mainEndpoint.getMethod()).thenReturn(endpointMethod);
		when(configuration.getDltHandlerMethod()).thenReturn(endpointHandlerMethod);

		RetryTopicConfigurer configurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver,
				listenerContainerFactoryConfigurer, beanFactory);

		// when
		configurer.processMainAndRetryListeners(endpointProcessor, mainEndpoint, configuration);

		// then
		verify(endpointProcessor, times(1))
				.accept(eq(mainEndpoint), mainCustomizerHolderCaptor.capture());
		RetryTopicConfigurer.EndpointProcessingCustomizerHolder mainHolder = mainCustomizerHolderCaptor.getValue();

		verify(destinationTopicProcessor, times(1))
				.processDestinationProperties(any(Consumer.class), any(DestinationTopicProcessor.Context.class));
	}

	@Test
	void shouldConfigure2() {

		// setup

		List<DestinationTopic.Properties> destinationPropertiesList =
				Arrays.asList(mainDestinationProperties, firstRetryDestinationProperties,
						secondRetryDestinationProperties, dltDestinationProperties);


		List<MethodKafkaListenerEndpoint> endpoints = Arrays.asList(mainEndpoint, retryEndpoint1, retryEndpoint2, dltEndpoint);

		IntStream.range(1, endpoints.size())
				.forEach(index -> {
			MethodKafkaListenerEndpoint endpoint = endpoints.get(index);
			when(endpoint.getId()).thenReturn("testId");
			when(endpoint.getGroup()).thenReturn("testGroup");
			when(endpoint.getGroupId()).thenReturn("testGroupId");
			when(endpoint.getClientIdPrefix()).thenReturn("testClientPrefix");
			when(endpoint.getTopics()).thenReturn(topics);
		});

		when(configuration.getDestinationTopicProperties()).thenReturn(destinationPropertiesList);
		when(mainEndpoint.getBean()).thenReturn(bean);
		when(mainEndpoint.getMethod()).thenReturn(endpointMethod);
		when(configuration.getDltHandlerMethod()).thenReturn(endpointHandlerMethod);

		when(configuration.getFactoryResolverConfig()).thenReturn(factoryResolverConfig);
		when(configuration.getDeadLetterProviderConfiguration()).thenReturn(deadLetterConfiguration);
		when(listenerContainerFactoryConfigurer.configure(containerFactory, deadLetterConfiguration))
				.thenReturn(containerFactory);
		when(containerFactoryResolver.resolveFactoryForMainEndpoint(any(KafkaListenerContainerFactory.class),
				eq(factoryResolverConfig))).thenReturn(containerFactory);
		when(firstRetryDestinationProperties.suffix()).thenReturn("-retry");
		when(secondRetryDestinationProperties.suffix()).thenReturn("-retry");
		when(dltDestinationProperties.suffix()).thenReturn("-dlt");

		RetryTopicConfigurer configurer = new RetryTopicConfigurer(destinationTopicProcessor, containerFactoryResolver,
				listenerContainerFactoryConfigurer, beanFactory);

		// when
		configurer.processMainAndRetryListeners(endpointProcessor, mainEndpoint, configuration);

		// then
		verify(endpointProcessor, times(1))
				.accept(eq(mainEndpoint), mainCustomizerHolderCaptor.capture());
		RetryTopicConfigurer.EndpointProcessingCustomizerHolder mainHolder = mainCustomizerHolderCaptor.getValue();

		MethodKafkaListenerEndpoint<?, ?> processedEndpoint = mainHolder.getEndpointCustomizer().apply(mainEndpoint);
		KafkaListenerContainerFactory<?> customizedFactory = mainHolder.getFactoryCustomizer().apply(containerFactory);

		verify(destinationTopicProcessor, times(1))
				.processDestinationProperties(destinationPropertiesProcessorCaptor.capture(), contextCaptor.capture());

		Consumer<DestinationTopic.Properties> destinationPropertiesConsumer = destinationPropertiesProcessorCaptor.getValue();
		destinationPropertiesConsumer.accept(firstRetryDestinationProperties);
		destinationPropertiesConsumer.accept(secondRetryDestinationProperties);
		destinationPropertiesConsumer.accept(dltDestinationProperties);

		verify(endpointProcessor, times(4))
				.accept(any(MethodKafkaListenerEndpoint.class), retryCustomizerHolderCaptor.capture());
		List<RetryTopicConfigurer.EndpointProcessingCustomizerHolder> allRetryHolders = retryCustomizerHolderCaptor.getAllValues();

		DestinationTopicProcessor.Context context = contextCaptor.getValue();

		IntStream.range(1, allRetryHolders.size())
				.boxed()
				.map(index -> {
					allRetryHolders.get(index).getEndpointCustomizer().apply(endpoints.get(index));
					return index;
				})
				.forEach(index -> verifyEndpointCustomizing(endpoints.get(index), destinationPropertiesList.get(index), context));

		KafkaListenerContainerFactory<?> customizedRetryFactory = allRetryHolders.get(1).getFactoryCustomizer().apply(containerFactory);

	}

	private void verifyEndpointCustomizing(MethodKafkaListenerEndpoint<?, ?> endpoint, DestinationTopic.Properties properties, DestinationTopicProcessor.Context context) {
		Suffixer suffixer = new Suffixer(properties.suffix());
		verify(endpoint, times(1)).setId(suffixer.maybeAddTo(endpoint.getId()));
		verify(endpoint, times(1)).setGroupId(suffixer.maybeAddTo(endpoint.getGroupId()));
		verify(endpoint, times(1)).setClientIdPrefix(suffixer.maybeAddTo(endpoint.getClientIdPrefix()));
		verify(endpoint, times(1)).setGroup(suffixer.maybeAddTo(endpoint.getGroup()));
		endpoint
				.getTopics()
				.stream()
				.forEach(topic ->
						verify(destinationTopicProcessor, times(1))
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
		doReturn(noOps).when(beanFactory).getBean(NoOpsClass.class);
		RetryTopicConfigurer.EndpointHandlerMethod handlerMethod =
				RetryTopicConfigurer.createHandlerMethodWith(NoOpsClass.class, noOpsMethodName);

		// when
		Object resolvedBean = handlerMethod.resolveBean(this.beanFactory);

		// then
		assertEquals(noOps, resolvedBean);

	}

	@Test
	void shouldInstantiateIfNotInContainer() {

		// setup
		String beanName = NoOpsClass.class.getSimpleName() + "-handlerMethod";
		when(defaultListableBeanFactory.getBean(beanName)).thenReturn(new NoOpsClass());
		doThrow(NoSuchBeanDefinitionException.class).when(defaultListableBeanFactory).getBean(NoOpsClass.class);
		RetryTopicConfigurer.EndpointHandlerMethod handlerMethod =
				RetryTopicConfigurer.createHandlerMethodWith(NoOpsClass.class, noOpsMethodName);

		// when
		Object resolvedBean = handlerMethod.resolveBean(this.defaultListableBeanFactory);

		// then
		verify(defaultListableBeanFactory)
				.registerBeanDefinition(eq(beanName), any(RootBeanDefinition.class));
		assertTrue(NoOpsClass.class.isAssignableFrom(resolvedBean.getClass()));

	}

	static class NoOpsClass {
		void noOpsMethod(){};
	}

	@Test
	void shouldLogConsumerRecordMessage() {
		RetryTopicConfigurer.LoggingDltListenerHandlerMethod method =
				new RetryTopicConfigurer.LoggingDltListenerHandlerMethod();
		method.logMessage(consumerRecordMessage);
		verify(consumerRecordMessage, times(0)).topic();
	}

	@Test
	void shouldNotLogObjectMessage() {
		RetryTopicConfigurer.LoggingDltListenerHandlerMethod method =
				new RetryTopicConfigurer.LoggingDltListenerHandlerMethod();
		method.logMessage(objectMessage);
	}

	@Test
	void builder() {
	}
}