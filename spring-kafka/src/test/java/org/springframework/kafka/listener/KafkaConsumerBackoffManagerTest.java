package org.springframework.kafka.listener;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ListenerContainerPartitionIdleEvent;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerBackoffManagerTest {

	private final String testListenerId = "testListenerId";

	@Mock
	KafkaListenerEndpointRegistry registry;

	@Mock
	MessageListenerContainer listenerContainer;

	Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

	String testTopic = "testTopic";

	int testPartition = 0;

	private TopicPartition topicPartition = new TopicPartition(testTopic, testPartition);

	@Mock
	private ListenerContainerPartitionIdleEvent partitionIdleEvent;

	@Test
	void shouldBackoffWhenDueTimestampIsLater() {

		// setup
		when(this.registry.getListenerContainer(testListenerId)).thenReturn(listenerContainer);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).plusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);

		// when
		KafkaBackoffException backoffException = assertThrows(KafkaBackoffException.class,
				() -> backoffManager.maybeBackoff(context));

		// then
		assertEquals(expectedTimestamp, backoffException.getDueTimestamp());
		assertEquals(testListenerId, backoffException.getListenerId());
		assertEquals(topicPartition, backoffException.getTopicPartition());
		assertEquals(context, backoffManager.getBackoff(topicPartition));
		verify(listenerContainer, times(1)).pausePartition(topicPartition);
	}

	@Test
	void shouldNotBackoffWhenDueTimestampIsPast() {

		// setup
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).minusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);

		// when
		backoffManager.maybeBackoff(context);

		// then
		assertNull(backoffManager.getBackoff(topicPartition));
		verify(listenerContainer, times(0)).pausePartition(topicPartition);
	}

	@Test
	void shouldDoNothingIfIdleBeforeDueTimestamp() {

		// setup
		when(this.partitionIdleEvent.getTopicPartition()).thenReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).plusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);
		backoffManager.addBackoff(context, topicPartition);

		// when
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		assertEquals(context, backoffManager.getBackoff(topicPartition));
		verify(listenerContainer, times(0)).resumePartition(topicPartition);
	}

	@Test
	void shouldResumePartitionIfIdleAfterDueTimestamp() {

		// setup
		when(this.registry.getListenerContainer(testListenerId)).thenReturn(listenerContainer);
		when(this.partitionIdleEvent.getTopicPartition()).thenReturn(topicPartition);
		KafkaConsumerBackoffManager backoffManager = new KafkaConsumerBackoffManager(registry, clock);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock).minusMinutes(5);
		String expectedTimestamp = dueTimestamp.format(
				RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		KafkaConsumerBackoffManager.Context context =
				backoffManager.createContext(dueTimestamp, testListenerId, topicPartition);
		backoffManager.addBackoff(context, topicPartition);

		// when
		backoffManager.onApplicationEvent(partitionIdleEvent);

		// then
		assertNull(backoffManager.getBackoff(topicPartition));
		verify(listenerContainer, times(1)).resumePartition(topicPartition);
	}
}