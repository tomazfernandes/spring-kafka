package org.springframework.kafka.retrytopic;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.PartitionPausingBackOffManagerFactory;
import org.springframework.retry.backoff.ThreadWaitSleeper;
import org.springframework.util.Assert;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RetryTopicComponents implements ApplicationContextAware {

	static final Map<Class<?>, String> componentBeanNames;

	private Map<Class<?>, Object> componentInstances = new ConcurrentHashMap<>();

	private ApplicationContext applicationContext;

	static {
		componentBeanNames = new HashMap<>();
		componentBeanNames.put(ListenerContainerFactoryResolver.class,
				RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_RESOLVER_NAME);
		componentBeanNames.put(DefaultDestinationTopicProcessor.class,
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_PROCESSOR_NAME);
		componentBeanNames.put(ListenerContainerFactoryConfigurer.class,
				RetryTopicInternalBeanNames.LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME);
		componentBeanNames.put(DeadLetterPublishingRecovererFactory.class,
				RetryTopicInternalBeanNames.DEAD_LETTER_PUBLISHING_RECOVERER_FACTORY_BEAN_NAME);
		componentBeanNames.put(RetryTopicConfigurer.class, RetryTopicInternalBeanNames.RETRY_TOPIC_CONFIGURER);
		componentBeanNames.put(DefaultDestinationTopicResolver.class,
				RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME);
		componentBeanNames.put(ThreadWaitSleeper.class, RetryTopicInternalBeanNames.BACKOFF_SLEEPER_BEAN_NAME);
		componentBeanNames.put(PartitionPausingBackOffManagerFactory.class,
				RetryTopicInternalBeanNames.INTERNAL_KAFKA_CONSUMER_BACKOFF_MANAGER_FACTORY);
		componentBeanNames.put(Clock.class, RetryTopicInternalBeanNames.INTERNAL_BACKOFF_CLOCK_BEAN_NAME);
		componentBeanNames.put(KafkaConsumerBackoffManager.class, RetryTopicInternalBeanNames.KAFKA_CONSUMER_BACKOFF_MANAGER);
		componentBeanNames.put(RetryTopicNamesProviderFactory.class, RetryTopicInternalBeanNames.RETRY_TOPIC_NAMES_PROVIDER_FACTORY);
		componentBeanNames.put(RetryTopicComponents.class, RetryTopicInternalBeanNames.RETRY_TOPIC_COMPONENTS);
	}

	public static Class<ListenerContainerFactoryResolver> listenerContainerFactoryResolver() {
		return ListenerContainerFactoryResolver.class;
	}

	public static Class<DefaultDestinationTopicProcessor> defaultDestinationTopicProcessor() {
		return DefaultDestinationTopicProcessor.class;
	}

	public static Class<ListenerContainerFactoryConfigurer> listenerContainerFactoryConfigurer() {
		return ListenerContainerFactoryConfigurer.class;
	}

	public static Class<DeadLetterPublishingRecovererFactory> deadLetterPublishingRecovererFactory() {
		return DeadLetterPublishingRecovererFactory.class;
	}

	public static Class<DefaultDestinationTopicResolver> defaultDestinationTopicResolver() {
		return DefaultDestinationTopicResolver.class;
	}

	public static Class<RetryTopicConfigurer> retryTopicConfigurer() {
		return RetryTopicConfigurer.class;
	}

	public static Class<ThreadWaitSleeper> threadWaitSleeper() {
		return ThreadWaitSleeper.class;
	}

	public static Class<KafkaConsumerBackoffManager> kafkaConsumerBackoffManager() {
		return KafkaConsumerBackoffManager.class;
	}

	public static Class<PartitionPausingBackOffManagerFactory> kafkaBackOffManagerFactory() {
		return PartitionPausingBackOffManagerFactory.class;
	}

	public static Class<RetryTopicNamesProviderFactory> retryTopicNamesProviderFactory() {
		return RetryTopicNamesProviderFactory.class;
	}

	public static String getComponentBeanName(Class<?> componentClass) {
		return componentBeanNames.get(componentClass);
	}

	public static <T> boolean isRetryTopicComponent(Class<T> componentToConfigure) {
		return componentBeanNames.containsKey(componentToConfigure);
	}

	@SuppressWarnings("unchecked")
	<T> T getComponentInstance(Class<T> componentClass) {
		Object cachedInstance = componentInstances.get(componentClass);
		if (cachedInstance != null) {
			return (T) cachedInstance;
		}
		String componentBeanName = componentBeanNames.get(componentClass);
		Assert.notNull(componentBeanName,
				"Component " + componentClass + " is not a registered RetryTopic component");
		T componentInstance = this.applicationContext.getBean(componentBeanName, componentClass);
		// No synchronization needed since it's a ConcurrentHashMap and this op is idempotent
		this.componentInstances.put(componentClass, componentInstance);
		return componentInstance;
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}
}
