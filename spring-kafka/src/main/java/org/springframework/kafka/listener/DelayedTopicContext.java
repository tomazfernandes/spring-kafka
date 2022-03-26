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

package org.springframework.kafka.listener;

import java.time.Duration;

import org.springframework.kafka.config.DelayPrecision;
import org.springframework.kafka.config.DelaySource;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.util.Assert;

/**
 * The delay context for a {@link KafkaListenerEndpoint}.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public final class DelayedTopicContext {

	private final Duration delay;

	private final DelaySource delaySource;

	private final DelayPrecision delayPrecision;

	private final String producerDueTimestampHeader;

	private final KafkaConsumerBackoffManager backoffManager;

	private DelayedTopicContext(Duration delay, DelaySource delaySource,
								DelayPrecision delayPrecision, String producerDueTimestampHeader,
								KafkaConsumerBackoffManager backoffManager) {
		this.delay = delay;
		this.delaySource = delaySource;
		this.delayPrecision = delayPrecision;
		this.producerDueTimestampHeader = producerDueTimestampHeader;
		this.backoffManager = backoffManager;
	}

	public static DelayedTopicContextBuilder builder() {
		return new DelayedTopicContextBuilder();
	}

	public Duration getDelay() {
		return this.delay;
	}

	public DelaySource getDelaySource() {
		return this.delaySource;
	}

	public DelayPrecision getDelayPrecision() {
		return this.delayPrecision;
	}

	public String getProducerDueTimestampHeader() {
		return this.producerDueTimestampHeader;
	}

	public KafkaConsumerBackoffManager getBackoffManager() {
		return this.backoffManager;
	}

	public static final class DelayedTopicContextBuilder {

		private Duration delay;

		private DelaySource delaySource;

		private String producerDueTimestampHeader;

		private DelayPrecision delayPrecision;

		private KafkaConsumerBackoffManager backoffManager;

		private DelayedTopicContextBuilder() {
		}

		public DelayedTopicContextBuilder delay(Duration delay) {
			this.delay = delay;
			return this;
		}

		public DelayedTopicContextBuilder delaySource(DelaySource delaySource) {
			this.delaySource = delaySource;
			return this;
		}

		public DelayedTopicContextBuilder delayPrecision(DelayPrecision delayPrecision) {
			this.delayPrecision = delayPrecision;
			return this;
		}

		public DelayedTopicContextBuilder producerDueTimestampHeader(String producerDueTimestampHeader) {
			this.producerDueTimestampHeader = producerDueTimestampHeader;
			return this;
		}

		public DelayedTopicContextBuilder kafkaBackOffManager(KafkaConsumerBackoffManager backoffManager) {
			this.backoffManager = backoffManager;
			return this;
		}

		public DelayedTopicContext build() {
			Assert.notNull(this.delay, "delay cannot be null");
			Assert.notNull(this.delaySource, "delaySource cannot be null");
			Assert.notNull(this.delayPrecision, "delayPrecision cannot be null");
			Assert.notNull(this.producerDueTimestampHeader, "producerDueTimestampHeader cannot be null");
			Assert.notNull(this.backoffManager, "kafkaConsumerBackOffManager cannot be null");
			return new DelayedTopicContext(this.delay, this.delaySource, this.delayPrecision,
					this.producerDueTimestampHeader, this.backoffManager);
		}
	}

}
