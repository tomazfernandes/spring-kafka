/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.listener.adapter;

import java.time.Duration;

import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.DelayedTopic;
import org.springframework.kafka.listener.AcknowledgingConsumerAwareMessageListener;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * An {@link AcknowledgingConsumerAwareMessageListener} implementation that delays
 * record consumption by backing off the record's partition until a given delay
 * past the record's creation timestamp.
 *
 * @param <K> the key.
 * @param <V> the value.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 *
 * @see KafkaConsumerBackoffManager
 * @see DelayedTopic
 */
public class DelayedMessageListenerAdapter<K, V>
		extends AbstractDelegatingMessageListenerAdapter<MessageListener<K, V>>
		implements AcknowledgingConsumerAwareMessageListener<K, V> {

	private final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));

	private static final int THIRTY = 30;

	private static final Duration DEFAULT_DELAY_VALUE = Duration.ofSeconds(THIRTY);

	private final String listenerId;

	private final KafkaConsumerBackoffManager kafkaConsumerBackoffManager;

	private volatile Duration delay = DEFAULT_DELAY_VALUE;

	public DelayedMessageListenerAdapter(MessageListener<K, V> delegate,
			KafkaConsumerBackoffManager kafkaConsumerBackoffManager, String listenerId) {

		super(delegate);
		Assert.notNull(kafkaConsumerBackoffManager, "kafkaConsumerBackoffManager cannot be null");
		Assert.notNull(listenerId, "listenerId cannot be null");
		this.kafkaConsumerBackoffManager = kafkaConsumerBackoffManager;
		this.listenerId = listenerId;
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> consumerRecord, @Nullable Acknowledgment acknowledgment,
								@Nullable Consumer<?, ?> consumer) throws KafkaBackoffException {

		this.kafkaConsumerBackoffManager
						.backOffIfNecessary(createContext(consumerRecord,
								consumerRecord.timestamp() + this.delay.toMillis(),
								consumer));
		invokeDelegateOnMessage(consumerRecord, acknowledgment, consumer);
	}

	/**
	 * Sets the amount of time the record will be delayed,
	 * counting from its creation timestamp.
	 *
	 * The delay value can be changed at runtime - mind that
	 * if consumption is already backed off, if the new value is greater than the
	 * previous value, consumption will be backed off until the new value.
	 *
	 * However, if the new value is less than the current value,
	 * the new value will be effective beginning in the next record.
	 * @param delay the delay value.
	 */
	public void setDelay(Duration delay) {
		Assert.notNull(delay, "Delay cannot be null");
		this.logger.debug(() -> String.format("Setting delay %s for listener id %s", delay, this.listenerId));
		this.delay = delay;
	}

	private void invokeDelegateOnMessage(ConsumerRecord<K, V> consumerRecord, Acknowledgment acknowledgment, Consumer<?, ?> consumer) {
		switch (this.delegateType) {
			case ACKNOWLEDGING_CONSUMER_AWARE:
				this.delegate.onMessage(consumerRecord, acknowledgment, consumer);
				break;
			case ACKNOWLEDGING:
				this.delegate.onMessage(consumerRecord, acknowledgment);
				break;
			case CONSUMER_AWARE:
				this.delegate.onMessage(consumerRecord, consumer);
				break;
			case SIMPLE:
				this.delegate.onMessage(consumerRecord);
		}
	}

	private KafkaConsumerBackoffManager.Context createContext(ConsumerRecord<K, V> data, long nextExecutionTimestamp,
			Consumer<?, ?> consumer) {

		return this.kafkaConsumerBackoffManager.createContext(nextExecutionTimestamp, this.listenerId,
				new TopicPartition(data.topic(), data.partition()), consumer);
	}

	/*
	 * Since the container uses the delegate's type to determine which method to call, we
	 * must implement them all.
	 */
	@Override
	public void onMessage(ConsumerRecord<K, V> data) {
		onMessage(data, null, null); // NOSONAR
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Acknowledgment acknowledgment) {
		onMessage(data, acknowledgment, null); // NOSONAR
	}

	@Override
	public void onMessage(ConsumerRecord<K, V> data, Consumer<?, ?> consumer) {
		onMessage(data, null, consumer);
	}
}
