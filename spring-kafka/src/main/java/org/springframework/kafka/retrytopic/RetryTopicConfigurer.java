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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.config.MultiMethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ListenerUtils;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicContainer;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicProcessor;
import org.springframework.kafka.support.AllowDenyCollectionManager;
import org.springframework.kafka.support.Suffixer;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;


/**
 *
 * <p>Configures main, retry and DLT topics based on a main endpoint and provided configurations to acomplish
 * a distributed retry / DLT pattern in a non-blocking fashion, at the expense of ordering guarantees.
 *
 * <p>To illustrate, if you have a "main-topic" topic, and wants a exponential backoff of 1000ms with a multiplier of 2 and 3 retry attempts,
 * it will create the main-topic-retry-1000, main-topic-retry-2000, main-topic-retry-4000 and main-topic-dlt topics.
 * Each record retried will have a back off timestamp header and, if consumption is attempted before that time, the partition consumption
 * is paused and a {@link org.springframework.kafka.listener.KafkaBackoffException} is thrown.
 *
 * <p>This logic is handled via a {@link org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter} that makes use of
 * a {@link org.springframework.kafka.listener.KafkaConsumerBackoffManager}.
 *
 * <p>The {@link SeekToCurrentErrorHandler} and {@link DeadLetterPublishingRecoverer} then forwards the message to the next topic,
 * using a {@link org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicResolver} to know the next topic and the delay for it.
 *
 * <p>Considering Kafka's partition ordering guarantees, and each topic having a fixed delay time, we know that the first
 * message consumed in a given retry topic partition will be the one with the earliest backoff timestamp for that partition, so by pausing the partition
 * we know we're not delaying message processing in other partitions longer than necessary.
 *
 *
 * <p>Usages:
 *
 * <p>There are two main ways for configuring the endpoints. The first is by providing one or more {@link org.springframework.context.annotation.Bean}s
 * in a {@link org.springframework.context.annotation.Configuration} annotated class, such as:
 *
 * <pre>
 *     <code>@Bean</code>
 *     <code>public RetryTopicConfigurer myRetryableTopic(KafkaTemplate&lt;String, Object&gt; template) {
 *         return RetryTopicConfigurer
 *                 .builder()
 *                 .create(template);
 *      }</code>
 * </pre>
 * <p>This will create retry and dlt topics for all topics in methods annotated with {@link org.springframework.kafka.annotation.KafkaListener},
 * as well as its consumers, using the default configurations. If message processing fails it will forward the message to the next topic
 * until it gets to the DLT topic.
 *
 * One or more {@link org.springframework.kafka.core.KafkaTemplate} are required for message forwarding.
 * You can provide more than one instance in a {@link Map} where the Key is the value class that template should handle.
 *
 * <p>For more fine-grained control over how to handle retrials for each topic, more then one bean can be provided, such as:
 *
 * <pre>
 *     <code>@Bean
 *     public RetryTopicConfigurer myRetryableTopic(KafkaTemplate&lt;String, MyPojo&gt; template) {
 *         return RetryTopicConfigurer
 *                 .builder()
 *                 .fixedBackoff(3000)
 *                 .maxAttempts(5)
 *                 .includeTopics("my-topic", "my-other-topic")
 *                 .create(template);
 *         }</code>
 * </pre>
 * <pre>
 *	   <code>@Bean
 *     public RetryTopicConfigurer myOtherRetryableTopic(KafkaTemplate&lt;String, MyPojo&gt; template) {
 *         return RetryTopicConfigurer
 *                 .builder()
 *                 .exponentialBackoff(1000, 2, 5000)
 *                 .maxAttempts(4)
 *                 .excludeTopics("my-topic", "my-other-topic")
 *                 .retryOn(MyException.class)
 *                 .create(template);
 *         }</code>
 * </pre>
 * <p>Some other options include: auto-creation of topics (configuring / enabling / disabling),
 * backoff, retryOn / notRetryOn / transversing as in {@link org.springframework.retry.support.RetryTemplate},
 * custom dlt listener beans, custom topic suffixes and providing specific listenerContainerFactories.
 *
 * <p>The other, non-exclusive way to configure the endpoints is through the convenient {@link org.springframework.kafka.annotation.RetryableTopic} annotation,
 * that can be placed on any {@link org.springframework.kafka.annotation.KafkaListener} annotated methods, such as:
 *
 * <pre>
 *     <code>@RetryableTopic(attempts = 3,
 *     		backoff = @Backoff(delay = 700, maxDelay = 12000, multiplier = 3))</code>
 *     <code>@KafkaListener(topics = "my-annotated-topic")
 *     public void processMessage(MyPojo message) {
 *        		// ... message processing
 *     }</code>
 *</pre>
 * <p> The same configurations are available in the annotation and the builder approaches, and both can be used concurrently.
 * In case the same method / topic can be handled by both, the annotation takes precedence.
 *
 * <p>DLT Handling:
 *
 * <p>The DLT handler method can be provided through the {@link RetryTopicConfigurerBuilder#dltHandlerMethod(Class, String)} method,
 * providing the class and method name that should handle the DLT topic. If a bean instance of this type is found in the {@link BeanFactory}
 * it is the instance used. If not an instance is created.
 *
 * <pre>
 *     <code>@Bean
 *     public RetryTopicConfigurer otherRetryTopic(KafkaTemplate&lt;Integer, MyPojo&gt; template) {
 *         return RetryTopicConfigurer
 *                 .builder()
 *                 .dltProcessor(MyCustomDltProcessor.class, "processDltMessage")
 *                 .create(template);
 *     }</code>
 *
 *     <code>@Component
 *     public class MyCustomDltProcessor {
 *
 *     		public void processDltMessage(MyPojo message) {
 *  	       // ... message processing, persistence, etc
 *     		}
 *     }</code>
 * </pre>
 *
 * The other way to provide the DLT handler method is through the {@link org.springframework.kafka.annotation.DltHandler} annotation,
 * that should be used within the same class as the correspondent {@link KafkaListener}.
 *
 * 	<pre>
 * 	    <code>@DltHandler
 *       public void processMessage(MyPojo message) {
 *          		// ... message processing, persistence, etc
 *       }</code>
 *</pre>
 *
 * If no DLT handler is provided, the default {@link LoggingDltListenerHandlerMethod} is used.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 *
 * @see RetryTopicConfigurerBuilder
 * @see org.springframework.kafka.annotation.RetryableTopic
 * @see KafkaListener
 * @see org.springframework.retry.annotation.Backoff
 * @see SeekToCurrentErrorHandler
 * @see DeadLetterPublishingRecoverer
 *
 */
public class RetryTopicConfigurer {

	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(RetryTopicConfigurer.class));

	/**
	 * The default method to handle messages in the DLT.
	 */
	public static final EndpointHandlerMethod DEFAULT_DLT_HANDLER = createHandlerMethodWith(LoggingDltListenerHandlerMethod.class,
			LoggingDltListenerHandlerMethod.DEFAULT_DLT_METHOD_NAME);

	private final DestinationTopicProcessor destinationTopicProcessor;
	private final ListenerContainerFactoryResolver containerFactoryResolver;
	private final ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer;
	private final KafkaTopicAutoCreationManager autoCreationManager;
	private final EndpointHandlerMethod dltEndpointHandlerMethod;
	private final AllowDenyCollectionManager<String> gatekeeper;

	private BeanFactory beanFactory; // Setup on endpointProcessing

	RetryTopicConfigurer(EndpointHandlerMethod dltEndpointHandlerMethod,
						AllowDenyCollectionManager<String> gatekeeper,
						DestinationTopicProcessor destinationTopicProcessor,
						ListenerContainerFactoryResolver containerFactoryResolver,
						ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer,
						KafkaTopicAutoCreationManager autoCreationManager) {
		this.dltEndpointHandlerMethod = dltEndpointHandlerMethod != null ? dltEndpointHandlerMethod : DEFAULT_DLT_HANDLER;
		this.gatekeeper = gatekeeper;
		this.destinationTopicProcessor = destinationTopicProcessor;
		this.containerFactoryResolver = containerFactoryResolver;
		this.listenerContainerFactoryConfigurer = listenerContainerFactoryConfigurer;
		this.autoCreationManager = autoCreationManager;
	}

	/**
	 * Entrypoint for creating and configuring the retry and dlt endpoints,
	 * as well as the container factory that will the corresponding listenerContainer.
	 *
	 * @param endpointProcessor the endpoint and factory configurers that will be called at the end of the
	 *                           {@link org.springframework.kafka.annotation.KafkaListenerAnnotationBeanPostProcessor} processListener method.
	 *                           As a side effect, the created destinations are registered at the {@link DestinationTopicContainer}
	 * @param mainEndpoint the endpoint based on which retry and dlt endpoints are also created and processed.
	 *
	 */
	public void processMainAndRetryListeners(EndpointProcessor endpointProcessor, MethodKafkaListenerEndpoint<?, ?> mainEndpoint) {
		throwIfMultiMethodEndpoint(mainEndpoint); // TODO: Add support to MultiMethodEndpoint
		configureMainEndpoint(mainEndpoint, endpointProcessor);
		configureRetryAndDltEndpoints(mainEndpoint, endpointProcessor);
		this.destinationTopicProcessor.processRegisteredDestinations(this.autoCreationManager::maybeCreateTopics);
	}

	private void configureMainEndpoint(MethodKafkaListenerEndpoint<?, ?> mainEndpoint, EndpointProcessor endpointProcessor) {
		EndpointProcessingCustomizerHolder holder =
				new EndpointProcessingCustomizerHolder(endpoint -> endpoint, this::resolveAndConfigureFactoryForMainEndpoint);
		endpointProcessor
				.accept(mainEndpoint, holder);
	}

	private void configureRetryAndDltEndpoints(MethodKafkaListenerEndpoint<?, ?> mainEndpoint, EndpointProcessor endpointProcessor) {
		EndpointHandlerMethod retryBeanMethod = createHandlerMethodWith(mainEndpoint.getBean(), mainEndpoint.getMethod());
		this.destinationTopicProcessor
				.processDestinationProperties(destinationTopicProperties ->
						endpointProcessor.accept(new MethodKafkaListenerEndpoint<>(),
								new EndpointProcessingCustomizerHolder(getEndpointCustomizerFunction(retryBeanMethod, destinationTopicProperties),
										this::resolveAndConfigureFactoryForRetryEndpoint)));
	}

	public Function<MethodKafkaListenerEndpoint<?, ?>, MethodKafkaListenerEndpoint<?, ?>> getEndpointCustomizerFunction(EndpointHandlerMethod retryBeanMethod, DestinationTopic.Properties destinationTopicProperties) {
		return new EndpointCustomizerFunctionFactory(destinationTopicProperties, getEndpointBeanMethod(destinationTopicProperties, retryBeanMethod),
						this.beanFactory, endpointCustomizingContext -> createAndRegisterDestinationTopic(destinationTopicProperties, endpointCustomizingContext))
				.createEndpointCustomizer();
	}

	private EndpointCustomizingContext createAndRegisterDestinationTopic(DestinationTopic.Properties destinationTopicProperties, EndpointCustomizingContext endpointCustomizingContext) {
		DestinationTopic destinationTopic = new DestinationTopic(endpointCustomizingContext.getProcessedTopic(), destinationTopicProperties);
		this.destinationTopicProcessor.registerTopicDestination(endpointCustomizingContext.getTopic(), destinationTopic);
		return endpointCustomizingContext;
	}

	private EndpointHandlerMethod getEndpointBeanMethod(DestinationTopic.Properties props, EndpointHandlerMethod retryBeanMethod) {
		return props.isDltTopic() ? this.dltEndpointHandlerMethod : retryBeanMethod;
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> resolveAndConfigureFactoryForMainEndpoint(KafkaListenerContainerFactory<?> providedFactory) {
		return this.listenerContainerFactoryConfigurer
				.configure(this.containerFactoryResolver.resolveFactoryForMainEndpoint(providedFactory));
	}

	private ConcurrentKafkaListenerContainerFactory<?, ?> resolveAndConfigureFactoryForRetryEndpoint(KafkaListenerContainerFactory<?> providedFactory) {
		return this.listenerContainerFactoryConfigurer
				.configure(this.containerFactoryResolver.resolveFactoryForRetryEndpoint(providedFactory));
	}

	public boolean shouldHandleTopics(String[] topics) {
		return this.gatekeeper.areAllowed(topics);
	}

	private void throwIfMultiMethodEndpoint(MethodKafkaListenerEndpoint<?, ?> mainEndpoint) {
		if (mainEndpoint instanceof MultiMethodKafkaListenerEndpoint) {
			throw new IllegalArgumentException("Retry Topic is not compatible with " + MultiMethodKafkaListenerEndpoint.class);
		}
	}

	public static EndpointHandlerMethod createHandlerMethodWith(Class<?> beanClass, String methodName) {
		return new EndpointHandlerMethod(beanClass, methodName);
	}

	public static EndpointHandlerMethod createHandlerMethodWith(Object bean, Method method) {
		return new EndpointHandlerMethod(bean, method);
	}

	public static RetryTopicConfigurerBuilder builder() {
		return new RetryTopicConfigurerBuilder();
	}

	RetryTopicConfigurer setBeanFactory(BeanFactory beanFactory) {
		// TODO: Probably there's a better way to setup these dependencies.
		this.beanFactory = beanFactory;
		this.containerFactoryResolver.setBeanFactory(beanFactory);
		this.listenerContainerFactoryConfigurer.setBeanFactory(beanFactory);
		this.autoCreationManager.setBeanFactory(beanFactory);
		return this;
	}

	public interface EndpointProcessor extends BiConsumer<MethodKafkaListenerEndpoint<?, ?>, EndpointProcessingCustomizerHolder> {
	}

	/**
	 *
	 * Creates the endpoint and factory customizers that will be called at the end of the
	 *  KafkaListenerAnnotationBeanPostProcessor's processListener(MethodKafkaListenerEndpoint, KafkaListener, Object, Object, String)}
	 *  method.
	 *
	 *  <p>As a side effect, registers the topics in the {@link DestinationTopicContainer}.
	 *
	 */

	public final static class EndpointProcessingCustomizerHolder {
		private final Function<MethodKafkaListenerEndpoint<?, ?>, MethodKafkaListenerEndpoint<?, ?>> endpointCustomizer;
		private final Function<KafkaListenerContainerFactory<?>, KafkaListenerContainerFactory<?>> factoryCustomizer;

		public EndpointProcessingCustomizerHolder(Function<MethodKafkaListenerEndpoint<?, ?>, MethodKafkaListenerEndpoint<?, ?>> endpointCustomizer, Function<KafkaListenerContainerFactory<?>, KafkaListenerContainerFactory<?>> factoryCustomizer) {
			this.endpointCustomizer = endpointCustomizer;
			this.factoryCustomizer = factoryCustomizer;
		}

		public Function<MethodKafkaListenerEndpoint<?, ?>, MethodKafkaListenerEndpoint<?, ?>> getEndpointCustomizer() {
			return this.endpointCustomizer;
		}

		public Function<KafkaListenerContainerFactory<?>, KafkaListenerContainerFactory<?>> getFactoryCustomizer() {
			return this.factoryCustomizer;
		}
	}

	public final static class EndpointCustomizerFunctionFactory {

		private final DestinationTopic.Properties destinationProperties;
		private final EndpointHandlerMethod beanMethod;
		private final BeanFactory beanFactory;
		private final Function<EndpointCustomizingContext, EndpointCustomizingContext> endpointCustomizingContextListener;

		private EndpointCustomizerFunctionFactory(DestinationTopic.Properties destinationProperties,
												EndpointHandlerMethod beanMethod,
												BeanFactory beanFactory,
												Function<EndpointCustomizingContext, EndpointCustomizingContext> endpointCustomizingContextListener) {
			this.destinationProperties = destinationProperties;
			this.beanMethod = beanMethod;
			this.beanFactory = beanFactory;
			this.endpointCustomizingContextListener = endpointCustomizingContextListener;
		}

		public Function<MethodKafkaListenerEndpoint<?, ?>, MethodKafkaListenerEndpoint<?, ?>> createEndpointCustomizer() {
			return addSuffixesAndRegisterDestination(this.destinationProperties.suffix())
					.andThen(setBeanAndMethod(this.beanMethod.getBeanFromFactory(this.beanFactory), this.beanMethod.getMethod()));
		}

		private Function<MethodKafkaListenerEndpoint<?, ?>, MethodKafkaListenerEndpoint<?, ?>> setBeanAndMethod(Object bean, Method method) {
			return endpoint -> {
				endpoint.setBean(bean);
				endpoint.setMethod(method);
				return endpoint;
			};
		}

		private Function<MethodKafkaListenerEndpoint<?, ?>, MethodKafkaListenerEndpoint<?, ?>> addSuffixesAndRegisterDestination(String topicSuffix) {
			Suffixer suffixer = new Suffixer(topicSuffix);
			return endpoint -> {
				endpoint.setId(suffixer.maybeAddTo(endpoint.getId()));
				endpoint.setGroupId(suffixer.maybeAddTo(endpoint.getGroupId()));
				endpoint.setTopics(customizeTopics(suffixer, endpoint).toArray(new String[0]));
				endpoint.setClientIdPrefix(suffixer.maybeAddTo(endpoint.getClientIdPrefix()));
				endpoint.setGroup(suffixer.maybeAddTo(endpoint.getGroup()));
				return endpoint;
			};
		}

		private Collection<String> customizeTopics(Suffixer suffixer, MethodKafkaListenerEndpoint<?, ?> endpoint) {
			return endpoint
					.getTopics()
					.stream()
					.map(topic -> new EndpointCustomizingContext(topic, suffixer.maybeAddTo(topic)))
					.map(this.endpointCustomizingContextListener)
					.map(EndpointCustomizingContext::getProcessedTopic)
					.collect(Collectors.toList());
		}
	}

	private final static class EndpointCustomizingContext {
		private final String topic;
		private final String processedTopic;

		private EndpointCustomizingContext(String topic, String processedTopic) {
			this.topic = topic;
			this.processedTopic = processedTopic;
		}

		String getTopic() {
			return this.topic;
		}

		String getProcessedTopic() {
			return this.processedTopic;
		}
	}

	static final class EndpointHandlerMethod {
		private final Class<?> beanClass;
		private final Method method;
		private Object bean;

		private EndpointHandlerMethod(Class<?> beanClass, String methodName) {
			Assert.notNull(beanClass, () -> "No destination bean class provided!");
			Assert.notNull(methodName, () -> "No method name for destination bean class provided!");
			this.method = Arrays.stream(ReflectionUtils.getDeclaredMethods(beanClass)) // TODO: Maybe cache this result
					.filter(method -> method.getName().equals(methodName))
					.findFirst()
					.orElseThrow(() -> new IllegalArgumentException(String.format("No method %s in class %s", methodName, beanClass)));
			this.beanClass = beanClass;
		}

		private EndpointHandlerMethod(Object bean, Method method) {
			Assert.notNull(bean, () -> "No bean for destination provided!");
			Assert.notNull(method, () -> "No method for destination bean class provided!");
			this.method = method;
			this.bean = bean;
			this.beanClass = bean.getClass();
		}

		public Object getBeanFromFactory(BeanFactory beanFactory) {
			if (this.bean == null) {
				try {
					this.bean = beanFactory.getBean(this.beanClass);
				}
				catch (NoSuchBeanDefinitionException e) {
					this.bean = BeanUtils.instantiateClass(this.beanClass);
				}
			}
			return this.bean;
		}

		public Method getMethod() {
			return this.method;
		}
	}

	static class LoggingDltListenerHandlerMethod {

		public static final String DEFAULT_DLT_METHOD_NAME = "logMessage";

		public void logMessage(Object message) {
			if (message instanceof ConsumerRecord) {
				logger.info(() -> "Received message in dlt listener: " + ListenerUtils.recordToString((ConsumerRecord<?, ?>) message));
			}
			else {
				logger.info(() -> "Received message in dlt listener.");
			}
		}
	}
}


