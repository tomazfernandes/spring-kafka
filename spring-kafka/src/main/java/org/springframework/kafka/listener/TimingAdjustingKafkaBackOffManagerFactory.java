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

package org.springframework.kafka.listener;

import java.time.Clock;

import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;

/**
 *
 * Creates a {@link KafkaConsumerBackoffManager} instance
 * with or without a {@link KafkaConsumerTimingAdjuster}.
 *
 * @author Tomaz Fernandes
 * @since 2.7
 */
public class TimingAdjustingKafkaBackOffManagerFactory extends AbstractKafkaBackOffManagerFactory {

	private boolean isTimingAdjustmentEnabled = true;

	private KafkaConsumerTimingAdjuster timingAdjustmentManager;

	private TaskExecutor taskExecutor;

	private final Clock clock;

	/**
	 * Constructs a factory instance that will create the {@link KafkaConsumerBackoffManager}
	 * instances with the provided {@link KafkaConsumerTimingAdjuster}.
	 *
	 * @param timingAdjustmentManager the {@link KafkaConsumerTimingAdjuster} to be used.
	 */
	public TimingAdjustingKafkaBackOffManagerFactory(KafkaConsumerTimingAdjuster timingAdjustmentManager) {
		this();
		this.setTimingAdjustmentManager(timingAdjustmentManager);
	}

	/**
	 * Constructs a factory instance that will create the {@link KafkaConsumerBackoffManager}
	 * instances with the provided {@link TaskExecutor} in its {@link KafkaConsumerTimingAdjuster}.
	 *
	 * @param timingAdjustmentManagerTaskExecutor the {@link TaskExecutor} to be used.
	 */
	public TimingAdjustingKafkaBackOffManagerFactory(TaskExecutor timingAdjustmentManagerTaskExecutor) {
		this();
		this.setTaskExecutor(timingAdjustmentManagerTaskExecutor);
	}

	/**
	 * Constructs a factory instance specifying whether or not timing adjustment is enabled
	 * for this factories {@link KafkaConsumerBackoffManager}.
	 *
	 * @param isTimingAdjustmentEnabled the {@link KafkaConsumerTimingAdjuster} to be used.
	 */
	public TimingAdjustingKafkaBackOffManagerFactory(boolean isTimingAdjustmentEnabled) {
		this();
		this.setIsTimingAdjustmentEnabled(isTimingAdjustmentEnabled);
	}

	/**
	 * Constructs a factory instance using the provided {@link ListenerContainerRegistry}.
	 *
	 * @param listenerContainerRegistry the {@link ListenerContainerRegistry} to be used.
	 */
	public TimingAdjustingKafkaBackOffManagerFactory(ListenerContainerRegistry listenerContainerRegistry) {
		super(listenerContainerRegistry);
		this.clock = Clock.systemUTC();
	}

	/**
	 * Constructs a factory instance with default dependencies.
	 */
	public TimingAdjustingKafkaBackOffManagerFactory() {
		super();
		this.clock = Clock.systemUTC();
	}

	/**
	 * Constructs an factory instance that will create the {@link KafkaConsumerBackoffManager}
	 * with the provided {@link Clock}.
	 * @param clock the clock instance to be used.
	 */
	public TimingAdjustingKafkaBackOffManagerFactory(Clock clock) {
		super();
		this.clock = clock;
	}

	/**
	 * Set this property to false if you don't want the resulting KafkaBackOffManager
	 * to adjust the precision of the topics' consumption timing.
	 *
	 * @param isTimingAdjustmentEnabled set to false to disable timing adjustment.
	 */
	public void setIsTimingAdjustmentEnabled(boolean isTimingAdjustmentEnabled) {
		this.isTimingAdjustmentEnabled = isTimingAdjustmentEnabled;
	}

	/**
	 * Sets the {@link WakingKafkaConsumerTimingAdjuster} that will be used
	 * with the resulting {@link KafkaConsumerBackoffManager}.
	 *
	 * @param timingAdjustmentManager the adjustmentManager to be used.
	 */
	public void setTimingAdjustmentManager(KafkaConsumerTimingAdjuster timingAdjustmentManager) {
		Assert.isTrue(this.isTimingAdjustmentEnabled, () -> "TimingAdjustment is disabled for this factory.");
		this.timingAdjustmentManager = timingAdjustmentManager;
	}

	/**
	 * Sets the {@link TaskExecutor} that will be used in the {@link KafkaConsumerTimingAdjuster}.
	 * @param taskExecutor the taskExecutor to be used.
	 */
	public void setTaskExecutor(TaskExecutor taskExecutor) {
		Assert.isTrue(this.isTimingAdjustmentEnabled, () -> "TimingAdjustment is disabled for this factory.");
		this.taskExecutor = taskExecutor;
	}

	@Override
	protected KafkaConsumerBackoffManager doCreateManager(ListenerContainerRegistry registry) {
		KafkaConsumerBackoffManager kafkaConsumerBackoffManager = getKafkaConsumerBackoffManager(registry);
		super.addApplicationListener(kafkaConsumerBackoffManager);
		return kafkaConsumerBackoffManager;
	}

	private KafkaConsumerBackoffManager getKafkaConsumerBackoffManager(ListenerContainerRegistry registry) {
		return this.isTimingAdjustmentEnabled
			? new KafkaConsumerBackoffManager(registry, getOrCreateBackOffTimingAdjustmentManager(), this.clock)
			: new KafkaConsumerBackoffManager(registry, this.clock);
	}

	private KafkaConsumerTimingAdjuster getOrCreateBackOffTimingAdjustmentManager() {
		if (this.timingAdjustmentManager != null) {
			return this.timingAdjustmentManager;
		}
		return new WakingKafkaConsumerTimingAdjuster(getOrCreateTimingAdjustmentThreadExecutor());
	}

	private TaskExecutor getOrCreateTimingAdjustmentThreadExecutor() {
		if (this.taskExecutor != null) {
			return this.taskExecutor;
		}
		ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
		taskExecutor.initialize();
		super.addApplicationListener((ApplicationListener<ContextClosedEvent>) event -> taskExecutor.shutdown());
		return taskExecutor;
	}
}
