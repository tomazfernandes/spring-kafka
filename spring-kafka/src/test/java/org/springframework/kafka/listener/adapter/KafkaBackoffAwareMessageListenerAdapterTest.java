package org.springframework.kafka.listener.adapter;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaBackoffAwareMessageListenerAdapterTest {

	private final String testTopic = "testTopic";
	private final int testPartition = 0;
	@Mock
	AcknowledgingConsumerAwareMessageListener delegate;

	@Mock
	Acknowledgment ack;

	@Mock
	ConsumerRecord data;

	@Mock
	Consumer consumer;

	@Mock
	Headers headers;

	@Mock
	Header header;

	@Captor
	ArgumentCaptor<LocalDateTime> timestampCaptor;

	@Mock
	KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

	@Mock
	KafkaConsumerBackoffManager.Context context;

	String listenerId = "testListenerId";

	@Test
	void shouldThrowIfMethodWithNoAckInvoked() {
		// setup
		KafkaBackoffAwareMessageListenerAdapter backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		// when - then
		assertThrows(UnsupportedOperationException.class, () ->  backoffAwareMessageListenerAdapter.onMessage(data));
	}

	@Test
	void shouldJustDelegateIfNoBackoffHeaderPresent() {

		// setup
		when(data.headers()).thenReturn(headers);
		when(headers.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP)).thenReturn(null);

		KafkaBackoffAwareMessageListenerAdapter backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		backoffAwareMessageListenerAdapter.onMessage(data, ack, consumer);

		verify(delegate, times(1)).onMessage(data, ack, consumer);
		verify(ack, times(1)).acknowledge();
	}

	@Test
	void shouldCallBackoffManagerIfBackoffHeaderIsPresent() {

		// setup
		when(data.headers()).thenReturn(headers);
		when(headers.lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP))
				.thenReturn(header);
		LocalDateTime dueTimestamp = LocalDateTime.now(clock);
		String dueTimestampString = dueTimestamp
				.format(RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
		when(header.value())
				.thenReturn(dueTimestampString.getBytes());
		when(data.topic()).thenReturn(testTopic);
		when(data.partition()).thenReturn(testPartition);
		TopicPartition topicPartition = new TopicPartition(testTopic, testPartition);

		when(kafkaConsumerBackoffManager.createContext(dueTimestamp, listenerId, topicPartition))
					.thenReturn(context);

		KafkaBackoffAwareMessageListenerAdapter backoffAwareMessageListenerAdapter =
				new KafkaBackoffAwareMessageListenerAdapter<>(delegate, kafkaConsumerBackoffManager, listenerId);

		// when
		backoffAwareMessageListenerAdapter.onMessage(data, ack, consumer);

		// then
		verify(kafkaConsumerBackoffManager, times(1))
				.createContext(timestampCaptor.capture(), eq(listenerId), eq(topicPartition));
		assertEquals(dueTimestamp, timestampCaptor.getValue());
		verify(kafkaConsumerBackoffManager, times(1))
				.maybeBackoff(context);

		verify(delegate, times(1)).onMessage(data, ack, consumer);
		verify(ack, times(1)).acknowledge();
	}
}