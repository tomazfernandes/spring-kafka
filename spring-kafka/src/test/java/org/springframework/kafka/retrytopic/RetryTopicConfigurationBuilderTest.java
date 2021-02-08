package org.springframework.kafka.retrytopic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class RetryTopicConfigurationBuilderTest {

	@Mock
	KafkaOperations kafkaOperations;

	@Mock
	ConcurrentKafkaListenerContainerFactory containerFactory;

	@Test
	void shouldExcludeTopics() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		String topic1 = "topic1";
		String topic2 = "topic2";
		String[] topicNames = {topic1, topic2};
		List<String> topicNamesList = Arrays.asList(topicNames);

		//when
		builder.excludeTopics(topicNamesList);
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		assertFalse(configuration.hasConfigurationForTopics(topicNames));
	}

	@Test
	void shouldSetFixedBackOffPolicy() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		builder.fixedBackOff(1000);

		//when
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		assertEquals(0, destinationTopicProperties.get(0).delay());
		assertEquals(1000, destinationTopicProperties.get(1).delay());
		assertEquals(1000, destinationTopicProperties.get(2).delay());
		assertEquals(0, destinationTopicProperties.get(3).delay());
	}

	@Test
	void shouldSetNoBackoffPolicy() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		builder.noBackoff();

		//when
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		assertEquals(0, destinationTopicProperties.get(0).delay());
		assertEquals(0, destinationTopicProperties.get(1).delay());
		assertEquals(0, destinationTopicProperties.get(2).delay());
		assertEquals(0, destinationTopicProperties.get(3).delay());
	}

	@Test
	void shouldSetUniformRandomBackOff() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		int minInterval = 1000;
		int maxInterval = 10000;
		builder.uniformRandomBackoff(minInterval, maxInterval);

		//when
		RetryTopicConfiguration configuration = builder.create(kafkaOperations);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		assertEquals(0, destinationTopicProperties.get(0).delay());
		assertTrue(minInterval < destinationTopicProperties.get(1).delay());
		assertTrue(destinationTopicProperties.get(1).delay() < maxInterval);
		assertTrue(minInterval < destinationTopicProperties.get(2).delay());
		assertTrue(destinationTopicProperties.get(2).delay() < maxInterval);
		assertEquals(0, destinationTopicProperties.get(3).delay());
	}

	@Test
	void shouldRetryOn() {
		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();

		// when
		RetryTopicConfiguration configuration = builder.retryOn(IllegalArgumentException.class).create(kafkaOperations);

		// then
		DestinationTopic destinationTopic = new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0));
		assertTrue(destinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(destinationTopic.shouldRetryOn(0, new IllegalStateException()));
	}

	@Test
	void shouldNotRetryOn() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();

		//when
		RetryTopicConfiguration configuration = builder.notRetryOn(IllegalArgumentException.class).create(kafkaOperations);

		// then
		DestinationTopic destinationTopic = new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0));
		assertFalse(destinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertTrue(destinationTopic.shouldRetryOn(0, new IllegalStateException()));
	}

	@Test
	void shouldSetGivenFactory() {

		// setup
		String factoryName = "factoryName";
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		RetryTopicConfiguration configuration = builder
				.listenerFactory(containerFactory)
				.listenerFactory(factoryName)
				.create(kafkaOperations);

		ListenerContainerFactoryResolver.Configuration config = configuration.getFactoryResolverConfig();
		Object factoryInstance = ReflectionTestUtils.getField(config, "factoryFromRetryTopicConfiguration");
		Object listenerContainerFactoryName = ReflectionTestUtils.getField(config, "listenerContainerFactoryName");

		assertEquals(containerFactory, factoryInstance);
		assertEquals(factoryName, listenerContainerFactoryName);

	}

	@Test
	void shouldSetNotAutoCreateTopics() {

		// setup
		RetryTopicConfigurationBuilder builder = new RetryTopicConfigurationBuilder();
		RetryTopicConfiguration configuration = builder
				.doNotAutoCreateRetryTopics()
				.create(kafkaOperations);

		RetryTopicConfiguration.TopicCreation config = configuration.forKafkaTopicAutoCreation();
		assertFalse(config.shouldCreateTopics());
	}
}