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

package org.springframework.kafka.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import org.springframework.core.annotation.AliasFor;
import org.springframework.kafka.config.DelayPrecision;
import org.springframework.kafka.config.DelaySource;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.core.DelayedKafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.support.KafkaHeaders;


/**
 * <p>Delay record consumption by an arbitrary amount. The record's partition will be
 * backed off by a {@link KafkaConsumerBackoffManager} until consumption time is due.
 * Two {@link DelaySource}s are available, which can be combined.</p>
 *
 * <p>With {@link DelaySource#CONSUMER} all records will be processed after the same
 * predetermined delay based on {@link ConsumerRecord#timestamp()}.</p>
 *
 * <p>With {@link DelaySource#PRODUCER}, the producer adds a {@link Header} with
 * the due timestamp for the record. The same delay should be used for a given partition
 * or topic, otherwise a record set to be processed further in the future may block
 * records that should be processed earlier.</p>
 *
 * <p>With {@link DelaySource#BOTH}, if a due timestamp header is present,
 * the record will be processed after the configured delay or the provided timestamp,
 * whichever is latest. Otherwise only the configured delay will apply.
 *
 * <p>An example follows:</p>
 *
 * <pre class="code">
 *
 * &#064;Component
 * public class MyConsumerDelayedListener {
 *
 *     &#064;DelayedTopic
 *     &#064;KafkaListener(topics = "myConsumerDelayedTopic")
 *     public void listen(String message) {
 *         logger.info("Received message at " + System.currentTimeMillis());
 *     }
 * }
 * </pre>
 *
 * <p>This will add the default delay of 30 seconds to each record, using the default
 * {@link DelaySource#CONSUMER} strategy.</p>
 *
 * <p>Another possible setup is using the {@link DelaySource#PRODUCER} and
 * {@link DelayedKafkaTemplate}, such as:</p>
 *
 * <pre class="code">
 * &#064;Component
 * public class MyProducerDelayedListener {
 *
 *     &#064;DelayedTopic(source = DelaySource.PRODUCER)
 *     &#064;KafkaListener(topics = "myProducerDelayedTopic")
 *     public void listen(String message,
 *             &#064;Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
 *             &#064;Header(KafkaHeaders.DUE_TIMESTAMP) String dueTimestamp) {
 *         logger.info("Message {} received in topic {} with timestamp {}",
 *                 message, receivedTopic, dueTimestamp);
 *     }
 * }
 *
 * &#064;Component
 * public class MyDelayedProducer {
 *
 *     &#064;Autowired
 *     private DelayedKafkaTemplate delayedKafkaTemplate;
 *
 *     public void sendDelayedMessage() {
 *         delayedKafkaTemplate.sendDelayed("myProducerDelayedTopic",
 *                 "My test message", Duration.ofSeconds(2));
 *     }
 * }
 * </pre>
 *
 * This will delay message consumption by 2 seconds.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
@Target({ ElementType.METHOD, ElementType.TYPE, ElementType.ANNOTATION_TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DelayedTopic {

	/**
	 * The amount of time record consumption should be delayed.
	 * Applied to {@link DelaySource#CONSUMER} and {@link DelaySource#BOTH}.
	 * @return the delay value.
	 */
	@AliasFor("delay")
	long value() default 30000;

	/**
	 * The amount of time record consumption should be delayed.
	 * Applied to {@link DelaySource#CONSUMER} and {@link DelaySource#BOTH}.
	 * @return the delay value.
	 */
	long delay() default 30000;

	/**
	 * The amount of time record consumption should be delayed.
	 * Must resolve to a {@link Long} representing the amount of time in milliseconds.
	 * Applied to {@link DelaySource#CONSUMER} and {@link DelaySource#BOTH}.
	 * @return the delay value.
	 */
	String delayExpression() default "";

	/**
	 * The bean name of the {@link KafkaConsumerBackoffManager} to be used to back off
	 * the partitions. If none is provided, a default
	 * {@link KafkaListenerConfigUtils#KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME} will
	 * be looked up, and if none is found a default one will be created and registered.
	 * @return the {@link KafkaConsumerBackoffManager} name.
	 */
	String kafkaConsumerBackOffManager() default "";

	/**
	 * The delay source. See {@link DelaySource} for clarification on each value.
	 * @return the {@link DelaySource}
	 */
	DelaySource source() default DelaySource.CONSUMER;

	/**
	 * The {@link Header} to be looked up containing the record's consumption due time.
	 * Applied to {@link DelaySource#PRODUCER} and {@link DelaySource#BOTH}.
	 * @return the due timestamp header name.
	 */
	String producerDueTimestampHeader() default KafkaHeaders.DUE_TIMESTAMP;

	/**
	 * The precision level for the delay. Higher precision means setting lower
	 * {@link ContainerProperties#setIdlePartitionEventInterval} and
	 * {@link ContainerProperties#setPollTimeout} values.
	 *
	 * Note that this only applies if these properties have their default values.
	 * If a different value is set, it will not be overridden.
	 *
	 * @return the delay precision
	 */
	DelayPrecision delayPrecision() default DelayPrecision.HIGHEST;

}
