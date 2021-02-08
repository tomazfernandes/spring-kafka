/*
 * Copyright 2016-2021 the original author or authors.
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

import org.springframework.kafka.core.KafkaOperations;

import java.util.Map;

/**
 *
 * Contains the methods used by the {@link org.springframework.kafka.listener.DeadLetterPublishingRecoverer} to
 * resolve the destination topics and backoff header.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 */
public interface DestinationTopicResolver {

	DestinationTopic resolveNextDestination(String topic, Integer attempt, Exception e);
	String resolveDestinationNextExecutionTime(String topic, Integer attempt, Exception e);
	void addDestinations(Map<String, DestinationTopicResolver.DestinationsHolder> sourceDestinationMapToAdd);
	KafkaOperations<?, ?> getKafkaOperationsFor(String topic);

	static DestinationsHolder holderFor(DestinationTopic sourceDestination, DestinationTopic nextDestination) {
		return new DestinationsHolder(sourceDestination, nextDestination);
	}

	static class DestinationsHolder {
		private final DestinationTopic sourceDestination;
		private final DestinationTopic nextDestination;

		DestinationsHolder(DestinationTopic sourceDestination, DestinationTopic nextDestination) {
			this.sourceDestination = sourceDestination;
			this.nextDestination = nextDestination;
		}

		protected DestinationTopic getNextDestination() {
			return nextDestination;
		}

		protected DestinationTopic getSourceDestination() {
			return sourceDestination;
		}
	}
}
