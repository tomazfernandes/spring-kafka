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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.kafka.core.KafkaAdmin;


/**
 * Class responsible for creating the topics in Kafka.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 */
public class KafkaTopicAutoCreationManager {

	private final boolean shouldCreateTopics;
	private final int numPartitions;
	private final short replicationFactor;
	private AdminClient adminClient;

	KafkaTopicAutoCreationManager(boolean shouldCreate, int numPartitions, short replicationFactor) {
		this.shouldCreateTopics = shouldCreate;
		this.numPartitions = numPartitions;
		this.replicationFactor = replicationFactor;
	}

	KafkaTopicAutoCreationManager() {
		this.shouldCreateTopics = true;
		this.numPartitions = 1;
		this.replicationFactor = 1;
	}

	KafkaTopicAutoCreationManager(boolean shouldCreateTopics) {
		this.shouldCreateTopics = shouldCreateTopics;
		this.numPartitions = 1;
		this.replicationFactor = 1;
	}

	private List<NewTopic> createNewTopics(Collection<String> topics) {
		return topics
				.stream()
				.map(this::newTopicWithName)
				.collect(Collectors.toList());
	}

	private NewTopic newTopicWithName(String name) {
		return new NewTopic(name, this.numPartitions, this.replicationFactor);
	}

	public void maybeCreateTopics(Collection<String> topics) {
		if (this.shouldCreateTopics) {
			List<NewTopic> topicsToCreate = createNewTopics(topics);
			// TODO: Maybe wait for the topic creation futures so the topics are already created when consumers are instantiated?
			this.adminClient.createTopics(topicsToCreate);
		}
	}

	public void setBeanFactory(BeanFactory beanFactory) {
		this.adminClient = AdminClient.create(beanFactory.getBean(KafkaAdmin.class).getConfigurationProperties());
	}
}
