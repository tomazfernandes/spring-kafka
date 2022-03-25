/*
 * Copyright 2018-2022 the original author or authors.
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

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.listener.KafkaBackOffManagerFactory;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.KafkaConsumerTimingAdjuster;
import org.springframework.kafka.listener.PartitionPausingBackOffManagerFactory;
import org.springframework.retry.backoff.ThreadWaitSleeper;
import org.springframework.util.Assert;

/**
 *
 * Bootstraps the {@link RetryTopicConfigurer} context, registering the dependency
 * beans and configuring the {@link org.springframework.context.ApplicationListener}s.
 *
 * Note that if a bean with the same name already exists in the context that one will
 * be used instead.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class RetryTopicBootstrapper {

	private static final LogAccessor logger = new LogAccessor(
			LogFactory.getLog(RetryTopicBootstrapper.class));

	private final ApplicationContext applicationContext;

	private final BeanFactory beanFactory;

	private static final List<Class<?>> componentsToRegister;

	static {
		componentsToRegister = Arrays.asList(ListenerContainerFactoryResolver.class,
				DefaultDestinationTopicProcessor.class,
				ListenerContainerFactoryConfigurer.class,
				DeadLetterPublishingRecovererFactory.class,
				RetryTopicConfigurer.class,
				DefaultDestinationTopicResolver.class,
				ThreadWaitSleeper.class,
				PartitionPausingBackOffManagerFactory.class,
				RetryTopicComponents.class
		);
	}

	public RetryTopicBootstrapper(ApplicationContext applicationContext, BeanFactory beanFactory) {
		if (!ConfigurableApplicationContext.class.isAssignableFrom(applicationContext.getClass()) ||
				!BeanDefinitionRegistry.class.isAssignableFrom(applicationContext.getClass())) {
			throw new IllegalStateException(String.format("ApplicationContext must be implement %s and %s interfaces. Provided: %s",
					ConfigurableApplicationContext.class.getSimpleName(),
					BeanDefinitionRegistry.class.getSimpleName(),
					applicationContext.getClass().getSimpleName()));
		}
		if (!SingletonBeanRegistry.class.isAssignableFrom(beanFactory.getClass())) {
			throw new IllegalStateException("BeanFactory must implement " + SingletonBeanRegistry.class +
					" interface. Provided: " + beanFactory.getClass().getSimpleName());
		}
		this.beanFactory = beanFactory;
		this.applicationContext = applicationContext;
	}

	public void bootstrapRetryTopic() {
		registerBeans();
		registerSingletons();
		addApplicationListeners();
		configureComponents();
	}

	private void registerBeans() {
		componentsToRegister
				.forEach(this::registerIfNotContains);

		// Register a RetryTopicNamesProviderFactory implementation only if none is already present in the context
		try {
			this.applicationContext.getBean(RetryTopicNamesProviderFactory.class);
		}
		catch (NoSuchBeanDefinitionException e) {
			((BeanDefinitionRegistry) this.applicationContext).registerBeanDefinition(
					RetryTopicInternalBeanNames.RETRY_TOPIC_NAMES_PROVIDER_FACTORY,
					new RootBeanDefinition(SuffixingRetryTopicNamesProviderFactory.class));
		}
	}

	private void registerSingletons() {
		registerSingletonIfNotContains(RetryTopicInternalBeanNames.INTERNAL_BACKOFF_CLOCK_BEAN_NAME, Clock::systemUTC);
		registerSingletonIfNotContains(RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER,
				this::createKafkaConsumerBackoffManager);
	}

	private void addApplicationListeners() {
		((ConfigurableApplicationContext) this.applicationContext)
				.addApplicationListener(this.applicationContext.getBean(
						RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME, DefaultDestinationTopicResolver.class));
	}

	private KafkaConsumerBackoffManager createKafkaConsumerBackoffManager() {
		KafkaBackOffManagerFactory factory = this.applicationContext
				.getBean(RetryTopicInternalBeanNames.INTERNAL_KAFKA_CONSUMER_BACKOFF_MANAGER_FACTORY,
						KafkaBackOffManagerFactory.class);
		if (ApplicationContextAware.class.isAssignableFrom(factory.getClass())) {
			((ApplicationContextAware) factory).setApplicationContext(this.applicationContext);
		}
		if (PartitionPausingBackOffManagerFactory.class.isAssignableFrom(factory.getClass())) {
			setupTimingAdjustingBackOffFactory((PartitionPausingBackOffManagerFactory) factory);
		}
		return factory.create();
	}

	private void setupTimingAdjustingBackOffFactory(PartitionPausingBackOffManagerFactory factory) {
		if (this.applicationContext.containsBean(RetryTopicInternalBeanNames.BACKOFF_TASK_EXECUTOR)) {
			factory.setTaskExecutor(this.applicationContext
					.getBean(RetryTopicInternalBeanNames.BACKOFF_TASK_EXECUTOR, TaskExecutor.class));
		}
		if (this.applicationContext.containsBean(
				RetryTopicInternalBeanNames.INTERNAL_BACKOFF_TIMING_ADJUSTMENT_MANAGER)) {
			factory.setTimingAdjustmentManager(this.applicationContext
					.getBean(RetryTopicInternalBeanNames.INTERNAL_BACKOFF_TIMING_ADJUSTMENT_MANAGER,
						KafkaConsumerTimingAdjuster.class));
		}
	}

	@SuppressWarnings("unchecked")
	private void configureComponents() {
		Stream.concat(this.applicationContext.getBeansOfType(RetryTopicComponentConfigurer.class).values().stream(),
						this.applicationContext.getBeansOfType(RetryTopicCompositeConfigurer.class)
						.values().stream().map(RetryTopicCompositeConfigurer::getConfigurers)
						.flatMap(Collection::stream))
				.forEach(this::doConfigureComponent);
	}

	private <T> void doConfigureComponent(RetryTopicComponentConfigurer<T> configurer) {
		Class<T> componentClass = configurer.configures();
		String beanName = RetryTopicComponents.getComponentBeanName(componentClass);
		Assert.notNull(beanName, "Component " + componentClass.getName()
				+ " not found for configurer " + configurer);
		T componentInstance = this.applicationContext.getBean(beanName, componentClass);
		logger.debug(() -> String.format("Applying configurer %s to component instance %s",
				configurer, componentInstance));
		configurer.configure(componentInstance);
	}

	private void registerIfNotContains(Class<?> beanClass) {
		String beanName = RetryTopicComponents.getComponentBeanName(beanClass);
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;
		if (!registry.containsBeanDefinition(beanName)) {
			registry.registerBeanDefinition(beanName,
					new RootBeanDefinition(beanClass));
		}
	}

	private void registerSingletonIfNotContains(String beanName, Supplier<Object> singletonSupplier) {
		if (!this.applicationContext.containsBeanDefinition(beanName)) {
			((SingletonBeanRegistry) this.beanFactory).registerSingleton(beanName, singletonSupplier.get());
		}
	}

}
