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

package org.springframework.kafka.retrytopic.destinationtopic;

import org.jetbrains.annotations.NotNull;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;


/**
 *
 * This class contains the destination topics and correlates them with their source via the static sourceDestinationMap.
 * The map is static so that different topic groups can share the same {@link org.springframework.kafka.config.KafkaListenerContainerFactory}
 * and its configured destinationResolver function in the {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer}.
 *
 * Implements the {@link DestinationTopicProcessor} and {@link DestinationTopicResolver} interfaces.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 *
 */
public class DestinationTopicContainer implements DestinationTopicResolver, ApplicationListener<ContextRefreshedEvent> {

	private final Map<String, DestinationTopic> sourceDestinationMap;
	private boolean containerClosed;

	public DestinationTopicContainer() {
		this.sourceDestinationMap = new ConcurrentHashMap<>();
		this.containerClosed = false;
	}

	@Override
	public String resolveDestinationNextExecutionTime(String topic) {
		return LocalDateTime.now()
				.plus(getDestinationFor(topic).getDestinationDelay(), ChronoUnit.MILLIS)
				.format(KafkaConsumerBackoffManager.DEFAULT_BACKOFF_TIMESTAMP_FORMATTER);
	}

	@Override
	public String resolveDestinationFor(String topic) {
		return getDestinationFor(topic).getDestinationName();
	}

	@Override
	public String resolveDltDestinationFor(String topic) {
		DestinationTopic destination = getDestinationFor(topic);
		return destination.isDltTopic()
				? destination.getDestinationName()
				: resolveDltDestinationFor(destination.getDestinationName());
	}

	private DestinationTopic getDestinationFor(String topic) {
		return containerClosed
				? doGetDestinationFor(topic)
				: getDestinationTopicSynchronized(topic);
	}

	@NotNull
	private DestinationTopic getDestinationTopicSynchronized(String topic) {
		synchronized (sourceDestinationMap) {
			return doGetDestinationFor(topic);
		}
	}

	@NotNull
	private DestinationTopic doGetDestinationFor(String topic) {
		return Objects.requireNonNull(sourceDestinationMap.get(topic), () -> "No destination found for topic: " + topic);
	}

	@Override
	public void addDestinations(Map<String, DestinationTopic> sourceDestinationMapToAdd) {
		if (containerClosed) {
			throw new IllegalStateException("Cannot add new destinations, "
					+ DestinationTopicContainer.class.getSimpleName() + " is already closed.");
		}
		synchronized (sourceDestinationMap) {
			sourceDestinationMap.putAll(sourceDestinationMapToAdd);
		}
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		this.containerClosed = true;
	}
}
