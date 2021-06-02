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

package org.springframework.kafka.retrytopic;

import java.util.Collection;
import java.util.function.Function;

import org.springframework.kafka.config.MethodKafkaListenerEndpoint;

/**
 * Customizes main, retry and DLT endpoints in the Retry Topic functionality
 * and returns the resulting topic names.
 *
 * @author Tomaz Fernandes
 * @since 2.7.2
 *
 * @see EndpointCustomizerFactory
 *
 */
interface EndpointCustomizer extends Function<MethodKafkaListenerEndpoint<?, ?>,
		Collection<EndpointCustomizer.TopicNamesHolder>> {

	/**
	 *
	 * Customizes the endpoint and returns the topic names generated for this endpoint.
	 *
	 * @param listenerEndpoint The main, retry or DLT endpoint to be customized.
	 * @return A collection containing the topic names generated for this endpoint.
	 */
	default Collection<TopicNamesHolder> customizeEndpointAndCollectTopics(MethodKafkaListenerEndpoint<?, ?> listenerEndpoint) {
		return apply(listenerEndpoint);
	}

	class TopicNamesHolder {

		private final String mainTopic;

		private final String customizedTopic;

		TopicNamesHolder(String mainTopic, String customizedTopic) {
			this.mainTopic = mainTopic;
			this.customizedTopic = customizedTopic;
		}

		String getMainTopic() {
			return this.mainTopic;
		}

		String getCustomizedTopic() {
			return this.customizedTopic;
		}
	}
}
