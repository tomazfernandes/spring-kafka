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

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.springframework.kafka.listener.KafkaConsumerBackoffManager;


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
public class DestinationTopicContainer implements DestinationTopicProcessor, DestinationTopicResolver {

	private static final Map<String, DestinationTopic> sourceDestinationMap;
	private final Map<String, List<DestinationTopic>> destinationsByTopicMap;

	static {
		sourceDestinationMap = new ConcurrentHashMap<>();
	}

	private final List<DestinationTopic.Properties> properties;
	private List<String> allTopicsNames;

	public DestinationTopicContainer(List<DestinationTopic.Properties> properties) {
		this.properties = properties;
		this.destinationsByTopicMap = new HashMap<>();
	}

	@Override
	public void processDestinationProperties(Consumer<DestinationTopic.Properties> destinationPropertiesProcessor) {
		this
				.properties
				.forEach(destinationPropertiesProcessor);
	}

	@Override
	public void registerTopicDestination(String mainTopic, DestinationTopic destinationTopic) {
		List<DestinationTopic> topicDestinations = this.destinationsByTopicMap.computeIfAbsent(mainTopic, this::newListWithMainTopic);
		topicDestinations.add(destinationTopic);
	}

	@Override
	public void processRegisteredDestinations(Consumer<Collection<String>> topicsConsumer) {
		Map<String, DestinationTopic> sourceDestinationMapForThisInstance = this.destinationsByTopicMap
				.values()
				.stream()
				.map(this::correlatePairSourceAndDestinationValues)
				.reduce(this::concatenateMaps)
				.orElseThrow(() -> new IllegalStateException("No destinations where provided for the Retry Topic configuration"));
		addToSourceDestinationMap(sourceDestinationMapForThisInstance);
		topicsConsumer.accept(getAllTopicsNames());
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
		synchronized (sourceDestinationMap) {
			return Objects.requireNonNull(sourceDestinationMap.get(topic), () -> "No destination found for topic: " + topic);
		}
	}

	private void addToSourceDestinationMap(Map<String, DestinationTopic> sourceDestinationMapToAdd) {
		synchronized (sourceDestinationMap) {
			sourceDestinationMap.putAll(sourceDestinationMapToAdd);
		}
	}

	private List<DestinationTopic> newListWithMainTopic(String newTopic) {
		List<DestinationTopic> newList = new ArrayList<>();
		newList.add(new DestinationTopic(newTopic, new DestinationTopic.Properties(0, "", false)));
		return newList;
	}

	private Map<String, DestinationTopic> concatenateMaps(Map<String, DestinationTopic> firstMap, Map<String, DestinationTopic> secondMap) {
		firstMap.putAll(secondMap);
		return firstMap;
	}

	private Map<String, DestinationTopic> correlatePairSourceAndDestinationValues(List<DestinationTopic> destinationList) {
		return IntStream
				.range(0, destinationList.size() - 1)
				.boxed()
				.collect(Collectors.toMap(index -> destinationList.get(index).getDestinationName(),
						index -> getNextDestinationTopic(destinationList, index)));
	}

	private DestinationTopic getNextDestinationTopic(List<DestinationTopic> destinationList, int index) {
		return destinationList.get(index + 1);
	}

	private List<String> getAllTopicsNames() {
		if (this.allTopicsNames == null) {
			this.allTopicsNames = this.destinationsByTopicMap
					.values()
					.stream()
					.flatMap(Collection::stream)
					.map(DestinationTopic::getDestinationName)
					.collect(Collectors.toList());
		}
		return this.allTopicsNames;
	}
}
