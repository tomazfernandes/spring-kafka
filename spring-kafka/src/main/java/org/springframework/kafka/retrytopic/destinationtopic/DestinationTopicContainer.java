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

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.retrytopic.RetryTopicHeaders;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

	private final Map<String, DestinationsHolder> destinationsHolderMap;
	private boolean containerClosed;
	private final Clock clock;

	public DestinationTopicContainer(Clock clock) {
		this.clock = clock;
		this.destinationsHolderMap = new ConcurrentHashMap<>();
		this.containerClosed = false;
	}

	@Override
	public DestinationTopic resolveNextDestination(String topic, Integer attempt, Exception e) {
		DestinationsHolder destinationsHolder = getDestinationHolderFor(topic);
		return destinationsHolder.getSourceDestination().isDltTopic()
				? handleDltProcessingFailure(destinationsHolder)
				: destinationsHolder.getSourceDestination().shouldRetryOn(attempt, e)
					? resolveRetryDestination(destinationsHolder)
					: resolveDltDestination(topic);
	}

	private DestinationTopic handleDltProcessingFailure(DestinationsHolder destinationsHolder) {
		return destinationsHolder.getSourceDestination().isAlwaysRetryOnDltFailure()
				? destinationsHolder.getSourceDestination()
				: destinationsHolder.getNextDestination();
	}

	private DestinationTopic resolveRetryDestination(DestinationsHolder destinationsHolder) {
		return destinationsHolder.getSourceDestination().isSingleTopicRetry()
				? destinationsHolder.getSourceDestination()
				: destinationsHolder.getNextDestination();
	}

	@Override
	public String resolveDestinationNextExecutionTime(String topic, Integer attempt, Exception e) {
		return LocalDateTime.now(clock)
				.plus(resolveNextDestination(topic, attempt, e).getDestinationDelay(), ChronoUnit.MILLIS)
				.format(RetryTopicHeaders.DEFAULT_BACKOFF_TIMESTAMP_HEADER_FORMATTER);
	}

	private DestinationTopic resolveDltDestination(String topic) {
		DestinationTopic destination = getDestinationFor(topic);
		return destination.isDltTopic()
				? destination
				: resolveDltDestination(destination.getDestinationName());
	}

	private DestinationTopic getDestinationFor(String topic) {
		return getDestinationHolderFor(topic).getNextDestination();
	}

	private DestinationsHolder getDestinationHolderFor(String topic) {
		return containerClosed
				? doGetDestinationFor(topic)
				: getDestinationTopicSynchronized(topic);
	}

	private DestinationsHolder getDestinationTopicSynchronized(String topic) {
		synchronized (destinationsHolderMap) {
			return doGetDestinationFor(topic);
		}
	}

	private DestinationsHolder doGetDestinationFor(String topic) {
		return Objects.requireNonNull(destinationsHolderMap.get(topic),
				() -> "No destination found for topic: " + topic);
	}

	private Optional<DestinationsHolder> maybeGetDestinationFor(String topic) {
		return Optional.ofNullable(destinationsHolderMap.get(topic));
	}

	@Override
	public void addDestinations(Map<String, DestinationTopicResolver.DestinationsHolder> sourceDestinationMapToAdd) {
		if (containerClosed) {
			throw new IllegalStateException("Cannot add new destinations, "
					+ DestinationTopicContainer.class.getSimpleName() + " is already closed.");
		}
		synchronized (destinationsHolderMap) {
			destinationsHolderMap.putAll(sourceDestinationMapToAdd);
		}
	}

	@Override
	public KafkaOperations<?, ?> getKafkaOperationsFor(String topic) {
		return getDestinationFor(topic).getKafkaOperations();
	}

	@Override
	public void onApplicationEvent(ContextRefreshedEvent event) {
		this.containerClosed = true;
	}
}
