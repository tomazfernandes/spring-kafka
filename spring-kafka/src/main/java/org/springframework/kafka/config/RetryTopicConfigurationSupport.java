/*
 * Copyright 2022 the original author or authors.
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

package org.springframework.kafka.config;

import java.util.HashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableRetryTopic;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.listener.PartitionPausingBackOffManagerFactory;
import org.springframework.kafka.retrytopic.DeadLetterPublishingRecovererFactory;
import org.springframework.kafka.retrytopic.DefaultDestinationTopicResolver;
import org.springframework.kafka.retrytopic.DestinationTopicProcessor;
import org.springframework.kafka.retrytopic.DestinationTopicResolver;
import org.springframework.kafka.retrytopic.ListenerContainerFactoryConfigurer;
import org.springframework.kafka.retrytopic.ListenerContainerFactoryResolver;
import org.springframework.kafka.retrytopic.RetryTopicBeanNames;
import org.springframework.kafka.retrytopic.RetryTopicBootstrapper;
import org.springframework.kafka.retrytopic.RetryTopicConfigurer;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;
import org.springframework.kafka.retrytopic.RetryTopicNamesProviderFactory;
import org.springframework.util.Assert;
import org.springframework.util.backoff.BackOff;

/**
 * This is the main class providing the configuration behind the non-blocking,
 * topic-based delayed retries feature. It is typically imported by adding
 * {@link EnableRetryTopic @EnableRetryTopic} to an application
 * {@link Configuration @Configuration} class. An alternative more advanced option
 * is to extend directly from this class and override methods as necessary, remembering
 * to add {@link Configuration @Configuration} to the subclass and {@link Bean @Bean}
 * to overridden {@link Bean @Bean} methods. For more details see the javadoc of
 * {@link EnableRetryTopic @EnableRetryTopic}.
 *
 * @author Tomaz Fernandes
 * @since 2.9
*/
public class RetryTopicConfigurationSupport {

	private final RetryTopicComponentFactory componentFactory = createComponentFactory();

	/**
	 * Return a global {@link RetryTopicConfigurer} for configuring retry topics
	 * for {@link KafkaListenerEndpoint} instances with a corresponding
	 * {@link org.springframework.kafka.retrytopic.RetryTopicConfiguration}.
	 * To configure it, consider overriding the {@link #configureRetryTopicConfigurer()}.
	 * @param kafkaConsumerBackoffManager the global {@link KafkaConsumerBackoffManager}.
	 * @param destinationTopicResolver the global {@link DestinationTopicResolver}.
	 * @param beanFactory the {@link BeanFactory}.
	 * @return the instance.
	 */
	@Bean(name = RetryTopicBeanNames.RETRY_TOPIC_CONFIGURER_BEAN_NAME)
	public RetryTopicConfigurer retryTopicConfigurer(@Qualifier(KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME)
																KafkaConsumerBackoffManager kafkaConsumerBackoffManager,
													@Qualifier(RetryTopicBeanNames.DESTINATION_TOPIC_RESOLVER_BEAN_NAME)
															DestinationTopicResolver destinationTopicResolver,
													BeanFactory beanFactory) {

		DestinationTopicProcessor destinationTopicProcessor = this.componentFactory
				.destinationTopicProcessor(destinationTopicResolver);
		DeadLetterPublishingRecovererFactory dlprf = this.componentFactory
				.deadLetterPublishingRecovererFactory(destinationTopicResolver);
		ListenerContainerFactoryConfigurer lcfc = this.componentFactory
				.listenerContainerFactoryConfigurer(kafkaConsumerBackoffManager,
						dlprf, this.componentFactory.internalRetryTopicClock());
		ListenerContainerFactoryResolver factoryResolver = this.componentFactory
				.listenerContainerFactoryResolver(beanFactory);
		RetryTopicNamesProviderFactory retryTopicNamesProviderFactory =
				this.componentFactory.retryTopicNamesProviderFactory();

		processDeadLetterPublishingContainerFactory(dlprf);
		processListenerContainerFactoryConfigurer(lcfc);

		RetryTopicConfigurer retryTopicConfigurer = this.componentFactory
				.retryTopicConfigurer(destinationTopicProcessor, lcfc,
						factoryResolver, retryTopicNamesProviderFactory);

		Consumer<RetryTopicConfigurer> configurerConsumer = configureRetryTopicConfigurer();
		Assert.notNull(configurerConsumer, "configureRetryTopicConfigurer cannot return null.");
		configurerConsumer.accept(retryTopicConfigurer);
		return retryTopicConfigurer;
	}

	/**
	 * Override this method if you need to configure the {@link RetryTopicConfigurer}.
	 * @return a {@link RetryTopicConfigurer} consumer.
	 */
	protected Consumer<RetryTopicConfigurer> configureRetryTopicConfigurer() {
		return retryTopicConfigurer -> {
		};
	}

	/**
	 * Internal method for processing the {@link DeadLetterPublishingRecovererFactory}.
	 * Consider overriding the {@link #configureDeadLetterPublishingContainerFactory()}
	 * method if further customization is required.
	 * @param deadLetterPublishingRecovererFactory the instance.
	 */
	private void processDeadLetterPublishingContainerFactory(
			DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory) {
		CustomizersConfigurer customizersConfigurer = new CustomizersConfigurer();
		configureCustomizers(customizersConfigurer);
		if (customizersConfigurer.deadLetterPublishingRecovererCustomizer != null) {
			deadLetterPublishingRecovererFactory
					.setDeadLetterPublishingRecovererCustomizer(customizersConfigurer
							.deadLetterPublishingRecovererCustomizer);
		}
		Consumer<DeadLetterPublishingRecovererFactory> dlprConsumer = configureDeadLetterPublishingContainerFactory();
		Assert.notNull(dlprConsumer, "configureDeadLetterPublishingContainerFactory must not return null");
		dlprConsumer.accept(deadLetterPublishingRecovererFactory);
	}

	/**
	 * Override this method to further configure the {@link DeadLetterPublishingRecovererFactory}.
	 * @return a {@link DeadLetterPublishingRecovererFactory} consumer.
	 */
	protected Consumer<DeadLetterPublishingRecovererFactory> configureDeadLetterPublishingContainerFactory() {
		return dlprf -> {
		};
	}

	/**
	 * Internal method for processing the {@link ListenerContainerFactoryConfigurer}.
	 * Consider overriding {@link #configureListenerContainerFactoryConfigurer()}
	 * if further customization is required.
	 * @param listenerContainerFactoryConfigurer the {@link ListenerContainerFactoryConfigurer} instance.
	 */
	private void processListenerContainerFactoryConfigurer(ListenerContainerFactoryConfigurer listenerContainerFactoryConfigurer) {
		CustomizersConfigurer customizersConfigurer = new CustomizersConfigurer();
		configureCustomizers(customizersConfigurer);
		BlockingRetriesConfigurer blockingRetriesConfigurer = new BlockingRetriesConfigurer();
		configureBlockingRetries(blockingRetriesConfigurer);
		if (blockingRetriesConfigurer.backOff != null) {
			listenerContainerFactoryConfigurer
					.setBlockingRetriesBackOff(blockingRetriesConfigurer.backOff);
		}
		if (blockingRetriesConfigurer.retryableExceptions != null) {
			listenerContainerFactoryConfigurer
					.setBlockingRetryableExceptions(blockingRetriesConfigurer.retryableExceptions);
		}
		if (customizersConfigurer.errorHandlerCustomizer != null) {
			listenerContainerFactoryConfigurer
					.setErrorHandlerCustomizer(customizersConfigurer.errorHandlerCustomizer);
		}
		if (customizersConfigurer.listenerContainerCustomizer != null) {
			listenerContainerFactoryConfigurer
					.setContainerCustomizer(customizersConfigurer.listenerContainerCustomizer);
		}
		configureListenerContainerFactoryConfigurer()
				.accept(listenerContainerFactoryConfigurer);
	}

	/**
	 * Override this method to further configure the {@link ListenerContainerFactoryConfigurer}.
	 * @return a {@link ListenerContainerFactoryConfigurer} consumer.
	 */
	protected Consumer<ListenerContainerFactoryConfigurer> configureListenerContainerFactoryConfigurer() {
		return lcfc -> {
		};
	}

	/**
	 * Override this method to configure blocking retries parameters
	 * such as exceptions to be retried and the {@link BackOff} to be used.
	 * @param blockingRetries a {@link BlockingRetriesConfigurer}.
	 */
	protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
	}

	/**
	 * Override this method to configure non-blocking retries parameters
	 * such as fatal exceptions.
	 * @param nonBlockingRetries a {@link NonBlockingRetriesConfigurer}.
	 */
	protected void configureNonBlockingRetries(NonBlockingRetriesConfigurer nonBlockingRetries) {
	}

	/**
	 * Override this method to configure customizers for components created
	 * by non-blocking retries' configuration.
	 * @param customizersConfigurer a {@link CustomizersConfigurer}.
	 */
	protected void configureCustomizers(CustomizersConfigurer customizersConfigurer) {
	}

	/**
	 * Return a global {@link DestinationTopicResolver} for resolving
	 * the {@link org.springframework.kafka.retrytopic.DestinationTopic}
	 * to which a given {@link org.apache.kafka.clients.consumer.ConsumerRecord}
	 * should be sent for retry.
	 *
	 * To configure it, consider overriding one of these other more
	 * fine-grained methods:
	 * <ul>
	 * <li>{@link #configureNonBlockingRetries} for configuring non-blocking retries.
	 * <li>{@link #customizeDestinationTopicResolver} for further customizing the component.
	 * <li>{@link #createComponentFactory} for providing a subclass instance.
	 * </ul>
	 *
	 * @return the instance.
	 */
	@Bean(name = RetryTopicBeanNames.DESTINATION_TOPIC_RESOLVER_BEAN_NAME)
	public DestinationTopicResolver destinationTopicResolver() {
		DestinationTopicResolver destinationTopicResolver = this.componentFactory.destinationTopicResolver();
		if (destinationTopicResolver instanceof DefaultDestinationTopicResolver) {
			DefaultDestinationTopicResolver ddtr = (DefaultDestinationTopicResolver) destinationTopicResolver;
			NonBlockingRetriesConfigurer configurer = new NonBlockingRetriesConfigurer();
			configureNonBlockingRetries(configurer);
			if (configurer.clearDefaultFatalExceptions) {
				ddtr.setClassifications(new HashMap<>(), true);
			}
			if (configurer.addToFatalExceptions != null) {
				Stream.of(configurer.addToFatalExceptions).forEach(ddtr::addNotRetryableExceptions);
			}
			if (configurer.removeFromFatalExceptions != null) {
				Stream.of(configurer.removeFromFatalExceptions).forEach(ddtr::removeClassification);
			}
		}
		Consumer<DestinationTopicResolver> resolverConsumer = customizeDestinationTopicResolver();
		Assert.notNull(resolverConsumer, "customizeDestinationTopicResolver must not return null");
		return destinationTopicResolver;
	}

	/**
	 * Override this method to configure the {@link DestinationTopicResolver}.
	 * @return a {@link DestinationTopicResolver} consumer.
	 */
	protected Consumer<DestinationTopicResolver> customizeDestinationTopicResolver() {
		return dtr -> {
		};
	}

	/**
	 * Provides the {@link KafkaConsumerBackoffManager} instance.
	 * Override this method to provide a customized implementation.
	 * A {@link PartitionPausingBackOffManagerFactory} can be used for that purpose,
	 * or a different implementation can be provided.
	 * @param registry the {@link ListenerContainerRegistry} to be used to fetch the
	 * {@link MessageListenerContainer} to be backed off.
	 * @return the instance.
	 */
	@Bean(name = KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME)
	public KafkaConsumerBackoffManager kafkaConsumerBackoffManager(
			@Qualifier(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
					ListenerContainerRegistry registry) {
		PartitionPausingBackOffManagerFactory factory = new PartitionPausingBackOffManagerFactory(registry);
		return factory.create();
	}

	/**
	 * Override this method if you want to provide a subclass
	 * of {@link RetryTopicComponentFactory} with different
	 * component implementations or subclasses.
	 * @return the instance.
	 */
	protected RetryTopicComponentFactory createComponentFactory() {
		return new RetryTopicComponentFactory();
	}

	@Deprecated
	@Bean(name = RetryTopicInternalBeanNames.RETRY_TOPIC_BOOTSTRAPPER)
	RetryTopicBootstrapper retryTopicBootstrapper(ApplicationContext context) {
		return new RetryTopicBootstrapper(context, context.getAutowireCapableBeanFactory());
	}

	public static class BlockingRetriesConfigurer {

		private BackOff backOff;

		private Class<? extends Exception>[] retryableExceptions;

		@SuppressWarnings("varargs")
		@SafeVarargs
		public final BlockingRetriesConfigurer retryOn(Class<? extends Exception>... exceptions) {
			this.retryableExceptions = exceptions;
			return this;
		}

		public BlockingRetriesConfigurer backOff(BackOff backoff) {
			this.backOff = backoff;
			return this;
		}
	}

	@SuppressWarnings("varargs")
	public static class NonBlockingRetriesConfigurer {

		private Class<? extends Exception>[] addToFatalExceptions;

		private Class<? extends Exception>[] removeFromFatalExceptions;

		private boolean clearDefaultFatalExceptions = false;

		@SafeVarargs
		public final NonBlockingRetriesConfigurer addToFatalExceptions(Class<? extends Exception>... exceptions) {
			this.addToFatalExceptions = exceptions;
			return this;
		}

		@SafeVarargs
		public final NonBlockingRetriesConfigurer removeFromFatalExceptions(Class<? extends Exception>... exceptions) {
			this.addToFatalExceptions = exceptions;
			return this;
		}

		public void clearDefaultFatalExceptions() {
			this.clearDefaultFatalExceptions = true;
		}
	}

	public static class CustomizersConfigurer {

		private Consumer<CommonErrorHandler> errorHandlerCustomizer;
		private Consumer<ConcurrentMessageListenerContainer<?, ?>> listenerContainerCustomizer;
		private Consumer<DeadLetterPublishingRecoverer> deadLetterPublishingRecovererCustomizer;

		protected CustomizersConfigurer customizeErrorHandler(Consumer<CommonErrorHandler> errorHandlerCustomizer) {
			this.errorHandlerCustomizer = errorHandlerCustomizer;
			return this;
		}

		protected CustomizersConfigurer customizeListenerContainer(Consumer<ConcurrentMessageListenerContainer<?, ?>> listenerContainerCustomizer) {
			this.listenerContainerCustomizer = listenerContainerCustomizer;
			return this;
		}

		protected CustomizersConfigurer customizeDeadLetterPublishingRecoverer(Consumer<DeadLetterPublishingRecoverer> dlprCustomizer) {
			this.deadLetterPublishingRecovererCustomizer = dlprCustomizer;
			return this;
		}
	}

}
