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

import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The delay source for backing off the record's partition.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public enum DelaySource {

	/**
	 * The consumer backs off the partition by applying a fixed delay to
	 * {@link ConsumerRecord#timestamp()}.
	 */
	CONSUMER,

	/**
	 * The consumer looks for a header added by the producer with a due timestamp
	 * and backs the partition off until then.
	 */
	PRODUCER,

	/**
	 * The record will be consumed after whichever is latest -
	 * the consumer delay value or the due timestamp header added by the producer, if any.
	 */
	BOTH

}
