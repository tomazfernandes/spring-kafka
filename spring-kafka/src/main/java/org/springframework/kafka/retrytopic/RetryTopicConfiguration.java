package org.springframework.kafka.retrytopic;

import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.kafka.support.AllowDenyCollectionManager;

import java.util.List;

/**
 * @author tomazlemos
 * @since 23/01/21
 */
public class RetryTopicConfiguration {

	private final List<DestinationTopic.Properties> destinationTopicProperties;
	private final DeadLetterPublishingRecovererProvider.Configuration deadLetterProviderConfiguration;
	private final RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod;
	private final TopicCreation kafkaTopicAutoCreation;
	private final AllowDenyCollectionManager<String> topicAllowListManager;
	private final ListenerContainerFactoryResolver.Configuration factoryResolverConfig;

	public RetryTopicConfiguration(List<DestinationTopic.Properties> destinationTopicProperties,
								   DeadLetterPublishingRecovererProvider.Configuration deadLetterProviderConfiguration,
								   RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod,
								   TopicCreation kafkaTopicAutoCreation,
								   AllowDenyCollectionManager<String> topicAllowListManager,
								   ListenerContainerFactoryResolver.Configuration factoryResolverConfig) {
		this.destinationTopicProperties = destinationTopicProperties;
		this.deadLetterProviderConfiguration = deadLetterProviderConfiguration;
		this.dltHandlerMethod = dltHandlerMethod;
		this.kafkaTopicAutoCreation = kafkaTopicAutoCreation;
		this.topicAllowListManager = topicAllowListManager;
		this.factoryResolverConfig = factoryResolverConfig;
	}

	public boolean hasConfigurationForTopics(String[] topics) {
		return this.topicAllowListManager.areAllowed(topics);
	}

	public TopicCreation forKafkaTopicAutoCreation() {
		return kafkaTopicAutoCreation;
	}

	public DeadLetterPublishingRecovererProvider.Configuration getDeadLetterProviderConfiguration() {
		return deadLetterProviderConfiguration;
	}

	public ListenerContainerFactoryResolver.Configuration getFactoryResolverConfig() {
		return factoryResolverConfig;
	}

	public RetryTopicConfigurer.EndpointHandlerMethod getDltHandlerMethod() {
		return dltHandlerMethod;
	}

	public List<DestinationTopic.Properties> getDestinationTopicProperties() {
		return destinationTopicProperties;
	}

	static class TopicCreation {

		private final boolean shouldCreateTopics;
		private final int numPartitions;
		private final short replicationFactor;

		TopicCreation(boolean shouldCreate, int numPartitions, short replicationFactor) {
			this.shouldCreateTopics = shouldCreate;
			this.numPartitions = numPartitions;
			this.replicationFactor = replicationFactor;
		}

		TopicCreation() {
			this.shouldCreateTopics = true;
			this.numPartitions = 1;
			this.replicationFactor = 1;
		}

		TopicCreation(boolean shouldCreateTopics) {
			this.shouldCreateTopics = shouldCreateTopics;
			this.numPartitions = 1;
			this.replicationFactor = 1;
		}

		public int getNumPartitions() {
			return numPartitions;
		}

		public short getReplicationFactor() {
			return replicationFactor;
		}

		public boolean shouldCreateTopics() {
			return shouldCreateTopics;
		}
	}
}
