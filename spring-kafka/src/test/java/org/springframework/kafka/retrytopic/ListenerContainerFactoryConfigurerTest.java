package org.springframework.kafka.retrytopic;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.ContainerCustomizer;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.ErrorHandler;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.listener.adapter.AbstractDelegatingMessageListenerAdapter;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.kafka.support.Acknowledgment;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ListenerContainerFactoryConfigurerTest {

	@Mock
	KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	@Mock
	DeadLetterPublishingRecovererFactory deadLetterPublishingRecovererFactory;

	@Mock
	DeadLetterPublishingRecovererFactory.Configuration recovererConfiguration;

	@Mock
	DeadLetterPublishingRecoverer recoverer;

	@Mock
	ConcurrentKafkaListenerContainerFactory<?, ?> containerFactory;

	@Mock
	ContainerProperties containerProperties;

	@Captor
	ArgumentCaptor<ErrorHandler> errorHandlerCaptor;

	ConsumerRecord<?, ?> record = new ConsumerRecord<>("test-topic", 1, 1234L, new Object(), new Object());

	List<ConsumerRecord<?, ?>> records = Collections.singletonList(record);

	@Mock
	Consumer<?, ?> consumer;

	@Mock
	ConcurrentMessageListenerContainer container;

	@Mock
	OffsetCommitCallback offsetCommitCallback;

	@Mock
	java.util.function.Consumer<ErrorHandler> errorHandlerCustomizer;

	@Captor
	ArgumentCaptor<ContainerCustomizer> containerCustomizerCaptor;

	@Mock
	AcknowledgingConsumerAwareMessageListener listener;

	@Captor
	ArgumentCaptor<AbstractDelegatingMessageListenerAdapter> listenerAdapterCaptor;

	@Mock
	ConsumerRecord data;

	@Mock
	Acknowledgment ack;

	@Captor
	ArgumentCaptor<LocalDateTime> timestampCaptor;

	@Captor
	ArgumentCaptor<String> listenerIdCaptor;

	@Mock
	java.util.function.Consumer<ConcurrentMessageListenerContainer<?, ?>> configurerContainerCustomizer;

	@Test
	void shouldSetupErrorHandling() {

		// setup
		when(containerFactory.getContainerProperties()).thenReturn(containerProperties);
		when(container.getContainerProperties()).thenReturn(containerProperties);
		when(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).thenReturn(recoverer);
		when(containerProperties.getAckMode()).thenReturn(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		when(containerProperties.getCommitCallback()).thenReturn(offsetCommitCallback);


		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		configurer.setErrorHandlerCustomizer(errorHandlerCustomizer);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		verify(containerFactory, times(1)).setErrorHandler(errorHandlerCaptor.capture());
		ErrorHandler errorHandler = errorHandlerCaptor.getValue();
		assertTrue(SeekToCurrentErrorHandler.class.isAssignableFrom(errorHandler.getClass()));
		SeekToCurrentErrorHandler seekToCurrent = (SeekToCurrentErrorHandler) errorHandler;

		RuntimeException ex = new RuntimeException();
		seekToCurrent.handle(ex, records, consumer, container);

		verify(recoverer, times(1)).accept(record, ex);
		verify(containerProperties, times(1))
				.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
		verify(consumer, times(1)).commitAsync(any(Map.class), eq(offsetCommitCallback));
		verify(errorHandlerCustomizer, times(1)).accept(errorHandler);

	}

	@Test
	void shouldNotOverrideIdlePartitionEventInterval() {

		// setup
		long idlePartitionInterval = 100L;
		when(containerFactory.getContainerProperties()).thenReturn(containerProperties);
		when(container.getContainerProperties()).thenReturn(containerProperties);
		when(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).thenReturn(recoverer);
		when(containerProperties.getIdlePartitionEventInterval()).thenReturn(idlePartitionInterval);
		when(containerProperties.getMessageListener()).thenReturn(listener);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		verify(containerFactory, times(1)).setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		verify(containerProperties, times(0)).setIdlePartitionEventInterval(anyLong());
	}

	@Test
	void shouldSetIdlePartitionEventIntervalIfNull() {

		// setup
		long idlePartitionInterval = 1000L;
		when(containerFactory.getContainerProperties()).thenReturn(containerProperties);
		when(container.getContainerProperties()).thenReturn(containerProperties);
		when(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).thenReturn(recoverer);
		when(containerProperties.getIdlePartitionEventInterval()).thenReturn(null);
		when(containerProperties.getMessageListener()).thenReturn(listener);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		verify(containerFactory, times(1)).setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);
		verify(containerProperties, times(1)).setIdlePartitionEventInterval(idlePartitionInterval);
	}

	@Test
	void shouldSetupMessageListenerAdapter() {

		// setup
		when(containerFactory.getContainerProperties()).thenReturn(containerProperties);
		when(container.getContainerProperties()).thenReturn(containerProperties);
		when(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).thenReturn(recoverer);
		when(containerProperties.getIdlePartitionEventInterval()).thenReturn(null);
		when(containerProperties.getMessageListener()).thenReturn(listener);
		RecordHeaders headers = new RecordHeaders();
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP,
				LocalDateTime.now().format(RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER).getBytes());
		when(data.headers()).thenReturn(headers);
		String testListenerId = "testListenerId";
		when(container.getListenerId()).thenReturn(testListenerId);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		configurer.setContainerCustomizer(configurerContainerCustomizer);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		verify(containerFactory, times(1)).setContainerCustomizer(containerCustomizerCaptor.capture());
		ContainerCustomizer containerCustomizer = containerCustomizerCaptor.getValue();
		containerCustomizer.configure(container);

		verify(container, times(1)).setupMessageListener(listenerAdapterCaptor.capture());
		KafkaBackoffAwareMessageListenerAdapter listenerAdapter = (KafkaBackoffAwareMessageListenerAdapter) listenerAdapterCaptor.getValue();
		listenerAdapter.onMessage(data, ack, consumer);

		verify(this.kafkaConsumerBackoffManager, times(1))
				.createContext(any(LocalDateTime.class), listenerIdCaptor.capture(), any(TopicPartition.class));
		assertEquals(testListenerId, listenerIdCaptor.getValue());
		verify(listener, times(1)).onMessage(data, ack, consumer);

		verify(this.configurerContainerCustomizer, times(1)).accept(container);
	}

	@Test
	void shouldCacheFactoryInstances() {

		// setup
		when(containerFactory.getContainerProperties()).thenReturn(containerProperties);
		when(deadLetterPublishingRecovererFactory.create(recovererConfiguration)).thenReturn(recoverer);

		// when
		ListenerContainerFactoryConfigurer configurer =
				new ListenerContainerFactoryConfigurer(kafkaConsumerBackoffManager, deadLetterPublishingRecovererFactory);
		ConcurrentKafkaListenerContainerFactory<?, ?> factory = configurer.configure(containerFactory, recovererConfiguration);
		ConcurrentKafkaListenerContainerFactory<?, ?> secondFactory = configurer.configure(containerFactory, recovererConfiguration);

		// then
		assertEquals(factory, secondFactory);
		verify(containerFactory, times(1)).setContainerCustomizer(any());
		verify(containerFactory, times(1)).setErrorHandler(any());
		verify(containerProperties, times(1)).setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
	}
}