/*
 * Copyright 2016-2022 the original author or authors.
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

package org.springframework.kafka.listener;

import java.util.List;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.springframework.kafka.KafkaException;
import org.springframework.util.backoff.BackOff;

/**
 * An error handler prepared to handle a {@link KafkaBackoffException}
 * thrown by the listener.
 *
 * @author Tomaz Fernandes
 * @since 2.8.2
 *
 */
public class KafkaBackOffAwareErrorHandler extends DefaultErrorHandler {

	public KafkaBackOffAwareErrorHandler() {
	}

	public KafkaBackOffAwareErrorHandler(BackOff backOff) {
		super(backOff);
	}

	public KafkaBackOffAwareErrorHandler(ConsumerRecordRecoverer recoverer) {
		super(recoverer);
	}

	public KafkaBackOffAwareErrorHandler(ConsumerRecordRecoverer recoverer, BackOff backOff) {
		super(recoverer, backOff);
	}

	@Override
	public void handleRemaining(Exception thrownException, List<ConsumerRecord<?, ?>> records, Consumer<?, ?> consumer, MessageListenerContainer container) {
		SeekUtils.seekOrRecover(thrownException, records, consumer, container, isCommitRecovered(), // NOSONAR
				getRecoveryStrategy(records, thrownException), this.logger, SeekUtils.isBackoffException(thrownException)
						? getKafkaBackOffExceptionLogLevel()
						: getLogLevel());
	}

	protected KafkaException.Level getKafkaBackOffExceptionLogLevel() {
		return KafkaException.Level.DEBUG;
	}
}
