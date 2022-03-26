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

package org.springframework.kafka.config;

import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;

/**
 *
 * The delay precision to be used with the {@link KafkaConsumerBackoffManager}.
 * The provided value will be set to
 * {@link ContainerProperties#setIdlePartitionEventInterval} and
 * {@link ContainerProperties#setPollTimeout} if these properties have their default
 * values. If any of these values is manually set, it will not be overridden.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public enum DelayPrecision {

	/**
	 * Lowest delay precision. Records are likely to be processed some time after
	 * their due time in exchange for a higher poll timeout.
	 * Better suited for larger delay values (over 20 seconds).
	 */
	LOWEST(5000),

	/**
	 * Medium delay precision. Better suited for medium delay values (over 5 seconds).
	 */
	MEDIUM(1000),

	/**
	 * Highest delay precision. Better suited for small delay values (up to 5 seconds).
	 */
	HIGHEST(100);

	private final long delay;

	DelayPrecision(long delay) {
		this.delay = delay;
	}

	public long getDelay() {
		return this.delay;
	}
}
