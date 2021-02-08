package org.springframework.kafka.retrytopic.destinationtopic;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.BinaryExceptionClassifierBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class DestinationTopicPropertiesFactoryTest {

	private final String retryTopicSuffix = "test-retry-suffix";
	private final String dltSuffix = "test-dlt-suffix";
	private final int maxAttempts = 4;
	private final int numPartitions = 0;
	private final RetryTopicConfiguration.FixedDelayTopicStrategy fixedDelayTopicStrategy =
			RetryTopicConfiguration.FixedDelayTopicStrategy.SINGLE_TOPIC;
	private final RetryTopicConfiguration.DltProcessingFailureStrategy dltProcessingFailureStrategy =
			RetryTopicConfiguration.DltProcessingFailureStrategy.ABORT;
	private final BackOffPolicy backOffPolicy = new FixedBackOffPolicy();
	private final BinaryExceptionClassifier classifier = new BinaryExceptionClassifierBuilder()
			.retryOn(IllegalArgumentException.class).build();

	@Mock
	private KafkaOperations kafkaOperations;

	@BeforeEach
	void setup() {
		((FixedBackOffPolicy) backOffPolicy).setBackOffPeriod(1000);
	}

	@Test
	void shouldCreateMainAndDltProperties() {
		// when
		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, 1,
						backOffPolicy, classifier, numPartitions, kafkaOperations, fixedDelayTopicStrategy, dltProcessingFailureStrategy).createProperties();

		// then
		assertTrue(propertiesList.size() == 2);
		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		assertEquals("", mainTopicProperties.suffix());
		assertFalse(mainTopicProperties.isDltTopic());
		DestinationTopic mainTopic = new DestinationTopic("mainTopic", mainTopicProperties);
		assertEquals(0L, mainTopic.getDestinationDelay());
		assertTrue(mainTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(mainTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(mainTopic.shouldRetryOn(0, new RuntimeException()));

		DestinationTopic.Properties dltProperties = propertiesList.get(1);
		assertDltTopic(dltProperties);
	}

	private void assertDltTopic(DestinationTopic.Properties dltProperties) {
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = new DestinationTopic("mainTopic", dltProperties);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertFalse(dltTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(dltTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(dltTopic.shouldRetryOn(0, new RuntimeException()));
	}

	@Test
	void shouldCreateTwoRetryPropertiesForMultipleBackoffValues() {
		// when
		ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
		backOffPolicy.setInitialInterval(1000);
		backOffPolicy.setMultiplier(2);
		int maxAttempts = 3;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations, fixedDelayTopicStrategy,
						dltProcessingFailureStrategy).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertTrue(propertiesList.size() == 4);
		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix + "-1000", firstRetryProperties.suffix());
		assertFalse(firstRetryProperties.isDltTopic());
		DestinationTopic firstRetryDestinationTopic = destinationTopicList.get(1);
		assertEquals(1000, firstRetryDestinationTopic.getDestinationDelay());
		assertEquals(numPartitions, firstRetryDestinationTopic.getDestinationPartitions());
		assertTrue(firstRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(firstRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(firstRetryDestinationTopic.shouldRetryOn(0, new RuntimeException()));

		DestinationTopic.Properties secondRetryProperties = propertiesList.get(2);
		assertEquals(retryTopicSuffix + "-2000", secondRetryProperties.suffix());
		assertFalse(secondRetryProperties.isDltTopic());
		DestinationTopic secondRetryDestinationTopic = destinationTopicList.get(2);
		assertEquals(2000, secondRetryDestinationTopic.getDestinationDelay());
		assertEquals(numPartitions, secondRetryDestinationTopic.getDestinationPartitions());
		assertTrue(secondRetryDestinationTopic.shouldRetryOn(0, new IllegalArgumentException()));
		assertFalse(secondRetryDestinationTopic.shouldRetryOn(maxAttempts, new IllegalArgumentException()));
		assertFalse(secondRetryDestinationTopic.shouldRetryOn(0, new RuntimeException()));

		assertDltTopic(propertiesList.get(3));
	}

	@Test
	void shouldCreateOneRetryPropertyForFixedBackoffWithSingleTopicStrategy() {

		// when
		FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
		backOffPolicy.setBackOffPeriod(1000);
		int maxAttempts = 5;

		List<DestinationTopic.Properties> propertiesList =
				new DestinationTopicPropertiesFactory(retryTopicSuffix, dltSuffix, maxAttempts,
						backOffPolicy, classifier, numPartitions, kafkaOperations, RetryTopicConfiguration.FixedDelayTopicStrategy.SINGLE_TOPIC,
						dltProcessingFailureStrategy).createProperties();

		List<DestinationTopic> destinationTopicList = propertiesList
				.stream()
				.map(properties -> new DestinationTopic("mainTopic" + properties.suffix(), properties))
				.collect(Collectors.toList());

		// then
		assertTrue(propertiesList.size() == 3);

		DestinationTopic.Properties mainTopicProperties = propertiesList.get(0);
		DestinationTopic mainDestinationTopic = destinationTopicList.get(0);
		assertTrue(mainDestinationTopic.isMainTopic());

		DestinationTopic.Properties firstRetryProperties = propertiesList.get(1);
		assertEquals(retryTopicSuffix, firstRetryProperties.suffix());
		DestinationTopic retryDestinationTopic = destinationTopicList.get(1);
		assertTrue(retryDestinationTopic.isSingleTopicRetry());
		assertEquals(1000, retryDestinationTopic.getDestinationDelay());

		DestinationTopic.Properties dltProperties = propertiesList.get(2);
		assertEquals(dltSuffix, dltProperties.suffix());
		assertTrue(dltProperties.isDltTopic());
		DestinationTopic dltTopic = destinationTopicList.get(2);
		assertEquals(0, dltTopic.getDestinationDelay());
		assertEquals(numPartitions, dltTopic.getDestinationPartitions());
	}
}