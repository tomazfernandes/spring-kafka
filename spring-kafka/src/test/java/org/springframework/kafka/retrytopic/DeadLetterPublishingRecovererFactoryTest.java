package org.springframework.kafka.retrytopic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.NestedRuntimeException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicResolver;
import org.springframework.util.concurrent.ListenableFuture;

import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeadLetterPublishingRecovererFactoryTest {

	private final Clock clock = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault());

	@Mock
	DestinationTopicResolver destinationTopicResolver;

	String testTopic = "test-topic";
	String testRetryTopic = "test-topic-retry-0";

	private final Object key = new Object();
	private final Object value = new Object();
	ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 2, 0, key, value);

	@Mock
	DestinationTopic destinationTopic;

	@Mock
	KafkaOperations<Object, Object> kafkaOperations;

	@Mock
	KafkaOperations<Object, Object> kafkaOperations2;

	@Mock
	ListenableFuture listenableFuture;

	@Captor
	private ArgumentCaptor<ProducerRecord> producerRecordCaptor;

	@Test
	void shouldSendMessage() {
		// setup
		RuntimeException e = new RuntimeException();
		when(destinationTopicResolver.resolveNextDestination(testTopic, 1, e)).thenReturn(destinationTopic);
		when(destinationTopic.isNoOpsTopic()).thenReturn(false);
		when(destinationTopic.getDestinationName()).thenReturn(testRetryTopic);
		when(destinationTopic.getDestinationPartitions()).thenReturn(3);
		when(destinationTopicResolver.resolveDestinationNextExecutionTime(testTopic, 1, e))
				.thenReturn(getFormattedNowTimestamp());
		doReturn(this.kafkaOperations2).when(destinationTopicResolver).getKafkaOperationsFor(testRetryTopic);
		when(kafkaOperations2.send(any(ProducerRecord.class))).thenReturn(listenableFuture);

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(this.consumerRecord, e);

		// then
		verify(kafkaOperations2, times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		assertEquals(testRetryTopic, producerRecord.topic());
		assertEquals(value, producerRecord.value());
		assertEquals(key, producerRecord.key());
		assertEquals(2, producerRecord.partition());

		// assert headers
		Header attemptsHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		assertNotNull(attemptsHeader);
		assertEquals(2, attemptsHeader.value()[0]);
		Header timestampHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP);
		assertNotNull(timestampHeader);
		assertEquals(getFormattedNowTimestamp(), new String(timestampHeader.value()));
	}

	@Test
	void shouldIncreaseAttempts() {

		// setup
		RuntimeException e = new RuntimeException();
		ConsumerRecord consumerRecord = new ConsumerRecord(testTopic, 0, 0, key, value);
		consumerRecord.headers().add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS, BigInteger.valueOf(1).toByteArray());

		when(destinationTopicResolver.resolveNextDestination(testTopic, 1, e)).thenReturn(destinationTopic);
		when(destinationTopic.isNoOpsTopic()).thenReturn(false);
		when(destinationTopic.getDestinationName()).thenReturn(testRetryTopic);
		when(destinationTopic.getDestinationPartitions()).thenReturn(1);
		when(destinationTopicResolver.resolveDestinationNextExecutionTime(testTopic, 1, e))
				.thenReturn(getFormattedNowTimestamp());
		doReturn(this.kafkaOperations2).when(destinationTopicResolver).getKafkaOperationsFor(testRetryTopic);
		when(kafkaOperations2.send(any(ProducerRecord.class))).thenReturn(listenableFuture);

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(consumerRecord, e);

		// then
		verify(kafkaOperations2, times(1)).send(producerRecordCaptor.capture());
		ProducerRecord producerRecord = producerRecordCaptor.getValue();
		Header attemptsHeader = producerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		assertNotNull(attemptsHeader);
		assertEquals(2, attemptsHeader.value()[0]);
	}

	@Test
	void shouldNotSendMessageIfNoOpsDestination() {
		// setup
		RuntimeException e = new RuntimeException();
		when(destinationTopicResolver.resolveNextDestination(testTopic, 1, e)).thenReturn(destinationTopic);
		when(destinationTopic.isNoOpsTopic()).thenReturn(true);
		when(destinationTopicResolver.resolveDestinationNextExecutionTime(testTopic, 1, e))
				.thenReturn(getFormattedNowTimestamp());

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		deadLetterPublishingRecoverer.accept(this.consumerRecord, e);

		// then
		verify(kafkaOperations2, times(0)).send(any(ProducerRecord.class));
	}

	@Test
	void shouldThrowIfKafkaBackoffException() {
		// setup
		RuntimeException e = new KafkaBackoffException("KBEx", null, "test-listener-id", getFormattedNowTimestamp());

		DeadLetterPublishingRecovererFactory.Configuration configuration =
				new DeadLetterPublishingRecovererFactory.Configuration(this.kafkaOperations);
		DeadLetterPublishingRecovererFactory factory = new DeadLetterPublishingRecovererFactory(this.destinationTopicResolver);

		// when
		DeadLetterPublishingRecoverer deadLetterPublishingRecoverer = factory.create(configuration);
		assertThrows(NestedRuntimeException.class, () -> deadLetterPublishingRecoverer.accept(this.consumerRecord, e));

		// then
		verify(kafkaOperations2, times(0)).send(any(ProducerRecord.class));
	}

	@Test
	void shouldThrowIfNoTemplateProvided() {
		assertThrows(IllegalArgumentException.class, () -> new DeadLetterPublishingRecovererFactory.Configuration(null));
	}

	private String getFormattedNowTimestamp() {
		return LocalDateTime.now(this.clock).format(RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
	}
}
