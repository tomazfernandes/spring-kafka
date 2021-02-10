/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.event;

import org.apache.kafka.common.TopicPartition;

/**
 * An event published when a consumer partition is paused.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 *
 */
public class ConsumerPartitionPausedEvent extends KafkaEvent {

	private static final long serialVersionUID = 1L;

	private final TopicPartition partition;

	/**
	 * Construct an instance with the provided source and partition.
	 * @param source the container instance that generated the event.
	 * @param container the container or the parent container if the container is a child.
	 * @param partition the partition.
	 * @since 2.7
	 */
	public ConsumerPartitionPausedEvent(Object source, Object container, TopicPartition partition) {
		super(source, container);
		this.partition = partition;
	}

	/**
	 * Return the paused partition.
	 * @return the partition.
	 */
	public TopicPartition getPartitions() {
		return this.partition;
	}

	@Override
	public String toString() {
		return "ConsumerPausedEvent [partitions=" + this.partition + "]";
	}

}
