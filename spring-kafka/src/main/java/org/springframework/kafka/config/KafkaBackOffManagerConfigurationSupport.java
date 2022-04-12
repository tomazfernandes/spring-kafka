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

import java.time.Clock;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.KafkaConsumerTimingAdjuster;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.PartitionPausingBackoffManager;
import org.springframework.kafka.listener.WakingKafkaConsumerTimingAdjuster;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 * This class provides configuration support for a global
 * {@link KafkaConsumerBackoffManager} instance. Consider overriding any of the
 * protected methods for providing different components or configuration.
 * This class is automatically imported by the
 * {@link org.springframework.kafka.annotation.EnableRetryTopic @EnableRetryTopic}
 * annotation.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public class KafkaBackOffManagerConfigurationSupport implements DisposableBean {

	private ThreadPoolTaskExecutor taskExecutor;

	/**
	 * Provides the {@link KafkaConsumerBackoffManager} instance.
	 * To customize it or any of the components, consider overriding
	 * one of the more fine graned methods:
	 * <ul>
	 *     <li>{@link #backOffManagerClock}</li>
	 *     <li>{@link #timingAdjuster}</li>
	 *     <li>{@link #timingAdjusterTaskExecutor}</li>
	 * </ul>
	 * @param registry the global {@link ListenerContainerRegistry} instance.
	 * @return the instance.
	 */
	@Bean(name = KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME)
	public KafkaConsumerBackoffManager kafkaConsumerBackoffManager(@Qualifier(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)
																ListenerContainerRegistry registry) {
		return new PartitionPausingBackoffManager(registry, timingAdjuster(timingAdjusterTaskExecutor()), backOffManagerClock());
	}

	/**
	 * Override this method to provide a different {@link Clock}
	 * instance to be used with the {@link KafkaConsumerBackoffManager}.
	 * @return the instance.
	 */
	protected Clock backOffManagerClock() {
		return Clock.systemUTC();
	}

	/**
	 * Override this method to provide a different {@link KafkaConsumerTimingAdjuster}
	 * to be used with the {@link KafkaConsumerBackoffManager}.
	 * @param taskExecutor the task executor.
	 * @return the instance.
	 */
	protected KafkaConsumerTimingAdjuster timingAdjuster(TaskExecutor taskExecutor) {
		return new WakingKafkaConsumerTimingAdjuster(taskExecutor);
	}

	/**
	 * Override this method to provide a different {@link TaskExecutor}
	 * to be used with the {@link KafkaConsumerTimingAdjuster}.
	 * @return the instance.
	 */
	protected TaskExecutor timingAdjusterTaskExecutor() {
		Assert.isNull(this.taskExecutor, "A TaskExecutor has already been set.");
		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.initialize();
		this.taskExecutor = executor;
		return executor;
	}

	@Override
	public void destroy() throws Exception {
		if (this.taskExecutor != null) {
			this.taskExecutor.shutdown();
		}
	}
}
