/*
 * Copyright 2018-2022 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.kafka.support.TopicPartitionOffset;

/**
 *
 * Creates the {@link EndpointCustomizer} that will be used by the {@link RetryTopicConfigurer}
 * to customize the main, retry and DLT endpoints.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7.2
 *
 * @see RetryTopicConfigurer
 * @see DestinationTopic.Properties
 * @see RetryTopicNamesProviderFactory.RetryTopicNamesProvider
 *
 */
public class EndpointCustomizerFactory {

	private final DestinationTopic.Properties destinationProperties;

	private final EndpointHandlerMethod beanMethod;

	private final BeanFactory beanFactory;

	private final RetryTopicNamesProviderFactory retryTopicNamesProviderFactory;

	EndpointCustomizerFactory(DestinationTopic.Properties destinationProperties, EndpointHandlerMethod beanMethod,
							BeanFactory beanFactory, RetryTopicNamesProviderFactory retryTopicNamesProviderFactory) {

		this.destinationProperties = destinationProperties;
		this.beanMethod = beanMethod;
		this.beanFactory = beanFactory;
		this.retryTopicNamesProviderFactory = retryTopicNamesProviderFactory;
	}

	public final EndpointCustomizer createEndpointCustomizer() {
		return addSuffixesAndMethod(this.destinationProperties, this.beanMethod.resolveBean(this.beanFactory),
				this.beanMethod.getMethod());
	}

	protected EndpointCustomizer addSuffixesAndMethod(DestinationTopic.Properties properties, Object bean, Method method) {
		RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider =
				this.retryTopicNamesProviderFactory.createRetryTopicNamesProvider(properties);
		return endpoint -> {
			Collection<EndpointCustomizer.TopicNamesHolder> topics = getTopicNames(namesProvider, endpoint);
			endpoint.setId(namesProvider.getEndpointId(endpoint));
			endpoint.setGroupId(namesProvider.getGroupId(endpoint));
			if (endpoint.getTopics().isEmpty() && endpoint.getTopicPartitionsToAssign() != null) {
				endpoint.setTopicPartitions(getTopicPartitions(properties, namesProvider, endpoint.getTopicPartitionsToAssign()));
			}
			else {
				endpoint.setTopics(topics.stream().map(EndpointCustomizer.TopicNamesHolder::getCustomizedTopic).toArray(String[]::new));
			}
			endpoint.setClientIdPrefix(namesProvider.getClientIdPrefix(endpoint));
			endpoint.setGroup(namesProvider.getGroup(endpoint));
			endpoint.setBean(bean);
			endpoint.setMethod(method);
			Boolean autoStartDltHandler = properties.autoStartDltHandler();
			if (autoStartDltHandler != null && properties.isDltTopic()) {
				endpoint.setAutoStartup(autoStartDltHandler);
			}
			return topics;
		};
	}

	private TopicPartitionOffset[] getTopicPartitions(DestinationTopic.Properties properties,
													RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider,
													TopicPartitionOffset[] topicPartitionOffsets) {
		return Stream.of(topicPartitionOffsets)
				.map(tpo -> new TopicPartitionOffset(namesProvider.getTopicName(tpo.getTopic()),
						tpo.getPartition() <= properties.numPartitions() ? tpo.getPartition() : 0,
						(Long) null))
				.toArray(TopicPartitionOffset[]::new);
	}

	protected Collection<EndpointCustomizer.TopicNamesHolder> getTopicNames(
			RetryTopicNamesProviderFactory.RetryTopicNamesProvider namesProvider,
			MethodKafkaListenerEndpoint<?, ?> endpoint) {

		return getTopics(endpoint)
				.stream()
				.map(topic -> new EndpointCustomizer.TopicNamesHolder(topic, namesProvider.getTopicName(topic)))
				.collect(Collectors.toList());
	}

	private Collection<String> getTopics(MethodKafkaListenerEndpoint<?, ?> endpoint) {
		Collection<String> topics = endpoint.getTopics();
		if (topics.isEmpty()) {
			TopicPartitionOffset[] topicPartitionsToAssign = endpoint.getTopicPartitionsToAssign();
			if (topicPartitionsToAssign != null && topicPartitionsToAssign.length > 0) {
				topics = Arrays.stream(topicPartitionsToAssign)
						.map(TopicPartitionOffset::getTopic)
						.distinct()
						.collect(Collectors.toList());
			}
		}

		if (topics.isEmpty()) {
			throw new IllegalStateException(
					String.format("No topics were provided for RetryTopicConfiguration for method %s in class %s.",
							endpoint.getMethod().getName(), endpoint.getBean().getClass().getSimpleName()));
		}
		return topics;
	}
}
