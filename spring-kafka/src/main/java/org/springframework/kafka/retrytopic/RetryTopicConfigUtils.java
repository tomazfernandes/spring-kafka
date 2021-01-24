package org.springframework.kafka.retrytopic;

import org.springframework.kafka.listener.KafkaConsumerBackoffManager;

/**
 * @author tomazlemos
 * @since 23/01/21
 */
public class RetryTopicConfigUtils {

	static final String DESTINATION_TOPIC_PROCESSOR_NAME = "internalDestinationTopicProcessor";
	static final String KAFKA_CONSUMER_BACKOFF_MANAGER = "internalKafkaConsumerBackoffManager";
	static final String RETRY_TOPIC_CONFIGURER = "internalRetryTopicConfigurer";
	static final String LISTENER_CONTAINER_FACTORY_RESOLVER_NAME = "internalListenerContainerFactoryResolver";
	static final String LISTENER_CONTAINER_FACTORY_CONFIGURER_NAME = "internalListenerContainerFactoryConfigurer";
	static final String DEAD_LETTER_PUBLISHING_RECOVERER_PROVIDER_NAME = "internalDeadLetterPublishingRecovererProvider";
	static final String DESTINATION_TOPIC_CONTAINER_NAME = "internalDestinationTopicContainer";

}
