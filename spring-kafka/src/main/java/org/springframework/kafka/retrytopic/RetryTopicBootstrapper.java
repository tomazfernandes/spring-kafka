package org.springframework.kafka.retrytopic;

import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.retrytopic.destinationtopic.DefaultDestinationTopicProcessor;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicContainer;

import java.time.Clock;

/**
 * @author tomazlemos
 * @since 23/01/21
 */
public class RetryTopicBootstrapper {

	private final ApplicationContext applicationContext;

	public RetryTopicBootstrapper(ApplicationContext applicationContext) {
		if (!ConfigurableApplicationContext.class.isAssignableFrom(applicationContext.getClass()) ||
			!BeanDefinitionRegistry.class.isAssignableFrom(applicationContext.getClass())) {
			throw new IllegalStateException(String.format("ApplicationContext must be implement %s and %s interfaces. Provided: %s",
					ConfigurableApplicationContext.class.getSimpleName(),
					BeanDefinitionRegistry.class.getSimpleName(),
					applicationContext.getClass().getSimpleName()));
		}
		this.applicationContext = applicationContext;
	}

	public void bootstrapRetryTopic() {
		registerBeans();
		configureDestinationTopicContainer();
		configureKafkaConsumerBackoffManager();
		configureBackoffClock();
	}

	private void configureBackoffClock() {
		if (!applicationContext.containsBeanDefinition(RetryTopicConfigUtils.INTERNAL_BACKOFF_CLOCK_NAME)) {
			((SingletonBeanRegistry) applicationContext).registerSingleton(
					RetryTopicConfigUtils.INTERNAL_BACKOFF_CLOCK_NAME, Clock.systemUTC());
		}
	}

	private void registerBeans() {
		registerIfNotContains(RetryTopicConfigUtils.LISTENER_CONTAINER_FACTORY_RESOLVER_NAME,
				ListenerContainerFactoryResolver.class);
		registerIfNotContains(RetryTopicConfigUtils.DESTINATION_TOPIC_PROCESSOR_NAME,
				DefaultDestinationTopicProcessor.class);
		registerIfNotContains(RetryTopicConfigUtils.LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME,
				ListenerContainerFactoryConfigurer.class);
		registerIfNotContains(RetryTopicConfigUtils.DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME,
				DeadLetterPublishingRecovererFactory.class);
		registerIfNotContains(RetryTopicConfigUtils.RETRY_TOPIC_CONFIGURER, RetryTopicConfigurer.class);
		registerIfNotContains(RetryTopicConfigUtils.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class);
		registerIfNotContains(RetryTopicConfigUtils.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class);

	}

	private void configureKafkaConsumerBackoffManager() {
		KafkaConsumerBackoffManager kafkaConsumerBackoffManager = this.applicationContext.getBean(
				RetryTopicConfigUtils.KAFKA_CONSUMER_BACKOFF_MANAGER, KafkaConsumerBackoffManager.class);
		((ConfigurableApplicationContext) this.applicationContext).addApplicationListener(kafkaConsumerBackoffManager);
	}

	private void configureDestinationTopicContainer() {
		DestinationTopicContainer destinationTopicContainer = this.applicationContext.getBean(
				RetryTopicConfigUtils.DESTINATION_TOPIC_CONTAINER_NAME, DestinationTopicContainer.class);
		((ConfigurableApplicationContext) this.applicationContext).addApplicationListener(destinationTopicContainer);
	}

	private void registerIfNotContains(String beanName, Class<?> beanClass) {
		BeanDefinitionRegistry registry = (BeanDefinitionRegistry) this.applicationContext;
		if (!registry.containsBeanDefinition(beanName)) {
			registry.registerBeanDefinition(beanName,
					new RootBeanDefinition(beanClass));
		}
	}
}