/*
 * Copyright 2021-2022 the original author or authors.
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

package org.springframework.kafka.listener.adapter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.DelayedTopic;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.DelaySource;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.DelayedKafkaTemplate;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.StopWatch;



/**
 * @author Tomaz Fernandes
 * @since 2.9
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { DelayedTopicIntegrationTests.FIRST_TOPIC,
		DelayedTopicIntegrationTests.SECOND_TOPIC,
		DelayedTopicIntegrationTests.THIRD_TOPIC,
		DelayedTopicIntegrationTests.FOURTH_TOPIC,
		DelayedTopicIntegrationTests.FIFTH_TOPIC })
@TestPropertySource(properties = "three.seconds=3000")
public class DelayedTopicIntegrationTests {

	private static final Logger logger = LoggerFactory.getLogger(DelayedTopicIntegrationTests.class);

	public final static String FIRST_TOPIC = "myDelayedTopic1";

	public final static String SECOND_TOPIC = "myDelayedTopic2";

	public final static String THIRD_TOPIC = "myDelayedTopic3";

	public final static String FOURTH_TOPIC = "myDelayedTopic4";

	public final static String FIFTH_TOPIC = "myDelayedTopic5";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private DelayedKafkaTemplate<String, String> delayedKafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	void shouldDelayFirstTopicBy2Seconds() {
		logger.debug("Sending message to topic " + FIRST_TOPIC);
		StopWatch watch = new StopWatch();
		watch.start();
		kafkaTemplate.send(FIRST_TOPIC, "Testing topic 1");
		assertThat(awaitLatch(latchContainer.countDownLatch1)).isTrue();
		watch.stop();
		logger.debug(FIRST_TOPIC + " took " + watch.getTotalTimeMillis() + " to run.");
		assertThat(watch.getTotalTimeMillis()).isGreaterThanOrEqualTo(2000);
	}

	@Test
	void shouldDelaySecondTopicBy2SecondsViaProducer() {
		logger.debug("Sending message to topic " + SECOND_TOPIC);
		StopWatch watch = new StopWatch();
		watch.start();
		delayedKafkaTemplate.sendDelayed(SECOND_TOPIC, "Testing topic 2", Duration.ofSeconds(2));
		assertThat(awaitLatch(latchContainer.countDownLatch2)).isTrue();
		watch.stop();
		logger.debug(SECOND_TOPIC + " took " + watch.getTotalTimeMillis() + " to run.");
		assertThat(watch.getTotalTimeMillis()).isGreaterThanOrEqualTo(2000);
	}

	@Test
	void shouldDelayThirdTopicFor3Seconds() {
		logger.debug("Sending message to topic " + THIRD_TOPIC);
		StopWatch watch = new StopWatch();
		watch.start();
		delayedKafkaTemplate.sendDelayed(THIRD_TOPIC, "Testing topic 3", Duration.ofSeconds(2));
		assertThat(awaitLatch(latchContainer.countDownLatch3)).isTrue();
		watch.stop();
		logger.debug(THIRD_TOPIC + " took " + watch.getTotalTimeMillis() + " to run.");
		assertThat(watch.getTotalTimeMillis()).isGreaterThanOrEqualTo(3000);
	}

	@Test
	void shouldDelayFourthTopicFor3Seconds() {
		logger.debug("Sending message to topic " + FOURTH_TOPIC);
		StopWatch watch = new StopWatch();
		watch.start();
		delayedKafkaTemplate.sendDelayed(FOURTH_TOPIC, "Testing topic 4", Duration.ofSeconds(3));
		assertThat(awaitLatch(latchContainer.countDownLatch4)).isTrue();
		watch.stop();
		logger.debug(FOURTH_TOPIC + " took " + watch.getTotalTimeMillis() + " to run.");
		assertThat(watch.getTotalTimeMillis()).isGreaterThanOrEqualTo(3000);
	}

	@Test
	void shouldRespectDifferentDelaysForSeparatePartitions() {
		logger.debug("Sending message to topic " + FIFTH_TOPIC);
		StopWatch watchPartition0 = new StopWatch();
		StopWatch watchPartition1 = new StopWatch();
		watchPartition0.start();
		watchPartition1.start();
		delayedKafkaTemplate.sendDelayed(FIFTH_TOPIC, 0, null,
				"Testing topic 5 - 0", Duration.ofSeconds(2));
		delayedKafkaTemplate.sendDelayed(FIFTH_TOPIC, 1, null,
				"Testing topic 5 - 1", Duration.ofSeconds(3));
		assertThat(awaitLatch(latchContainer.countDownLatch5_0)).isTrue();
		watchPartition0.stop();
		assertThat(watchPartition0.getTotalTimeMillis()).isGreaterThanOrEqualTo(2000);
		assertThat(awaitLatch(latchContainer.countDownLatch5_1)).isTrue();
		watchPartition1.stop();
		assertThat(watchPartition1.getTotalTimeMillis()).isGreaterThanOrEqualTo(3000);
		logger.debug(FIFTH_TOPIC + " took " + watchPartition1.getTotalTimeMillis() + " to run.");
	}

	private boolean awaitLatch(CountDownLatch latch) {
		try {
			return latch.await(120, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			fail(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	static class FirstTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@DelayedTopic(delayExpression = "${missing.property:2000}")
		@KafkaListener(topics = FIRST_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			logger.debug("Message {} received in topic {}", message, receivedTopic);
			container.countDownLatch1.countDown();
		}
	}

	static class SecondTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@DelayedTopic(source = DelaySource.PRODUCER)
		@KafkaListener(topics = SECOND_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
								@Header(KafkaHeaders.DUE_TIMESTAMP) String dueTimestamp) {
			logger.debug("Message {} received in topic {} with timestamp {}", message, receivedTopic, dueTimestamp);
			container.countDownLatch2.countDown();
		}
	}

	static class ThirdTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@DelayedTopic(delayExpression = "${three.seconds}", source = DelaySource.BOTH)
		@KafkaListener(topics = THIRD_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
								@Header(KafkaHeaders.DUE_TIMESTAMP) String dueTimestamp) {
			logger.debug("Message {} received in topic {} with timestamp {}", message, receivedTopic, dueTimestamp);
			container.countDownLatch3.countDown();
		}
	}

	static class FourthTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@DelayedTopic(delayExpression = "#{2000}", source = DelaySource.BOTH)
		@KafkaListener(topics = FOURTH_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
								@Header(KafkaHeaders.DUE_TIMESTAMP) String dueTimestamp) {
			logger.debug("Message {} received in topic {} with timestamp {}", message, receivedTopic, dueTimestamp);
			container.countDownLatch4.countDown();
		}
	}

	static class FifthTopicListener {

		@Autowired
		CountDownLatchContainer container;

		@DelayedTopic(source = DelaySource.PRODUCER)
		@KafkaListener(topics = FIFTH_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
						@Header(KafkaHeaders.DUE_TIMESTAMP) String dueTimestamp,
						@Header(KafkaHeaders.RECEIVED_PARTITION_ID) Integer partition) {
			logger.debug("Message {} received in topic {} partition {} with timestamp {}", message, receivedTopic,
					partition, dueTimestamp);
			if (partition == 0) {
				container.countDownLatch5_0.countDown();
			}
			else {
				container.countDownLatch5_1.countDown();
			}
		}
	}

	static class CountDownLatchContainer {

		CountDownLatch countDownLatch1 = new CountDownLatch(1);
		CountDownLatch countDownLatch2 = new CountDownLatch(1);
		CountDownLatch countDownLatch3 = new CountDownLatch(1);
		CountDownLatch countDownLatch4 = new CountDownLatch(1);
		CountDownLatch countDownLatch5_0 = new CountDownLatch(1);
		CountDownLatch countDownLatch5_1 = new CountDownLatch(1);

	}

	@Configuration
	static class DelayedTopicTestConfiguration {

		@Bean
		public FirstTopicListener firstTopicListener() {
			return new FirstTopicListener();
		}

		@Bean
		public SecondTopicListener secondTopicListener() {
			return new SecondTopicListener();
		}

		@Bean
		public ThirdTopicListener thirdTopicListener() {
			return new ThirdTopicListener();
		}

		@Bean
		public FourthTopicListener fourthTopicListener() {
			return new FourthTopicListener();
		}

		@Bean
		public FifthTopicListener fifthTopicListener() {
			return new FifthTopicListener();
		}

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

	}

	@Configuration
	public static class KafkaProducerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		public ProducerFactory<String, String> producerFactory() {
			Map<String, Object> configProps = new HashMap<>();
			configProps.put(
					ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.broker.getBrokersAsString());
			configProps.put(
					ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			configProps.put(
					ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					StringSerializer.class);
			return new DefaultKafkaProducerFactory<>(configProps);
		}

		@Bean
		public KafkaTemplate<String, String> kafkaTemplate() {
			return new KafkaTemplate<>(producerFactory());
		}

		@Bean
		public DelayedKafkaTemplate<String, String> delayedKafkaTemplate() {
			return new DelayedKafkaTemplate<>(producerFactory());
		}
	}

	@EnableKafka
	@Configuration
	public static class KafkaConsumerConfig {

		@Autowired
		EmbeddedKafkaBroker broker;

		@Bean
		public KafkaAdmin kafkaAdmin() {
			Map<String, Object> configs = new HashMap<>();
			configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.broker.getBrokersAsString());
			return new KafkaAdmin(configs);
		}

		@Bean
		public ConsumerFactory<String, String> consumerFactory() {
			Map<String, Object> props = new HashMap<>();
			props.put(
					ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
					this.broker.getBrokersAsString());
			props.put(
					ConsumerConfig.GROUP_ID_CONFIG,
					"groupId");
			props.put(
					ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);
			props.put(
					ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
					StringDeserializer.class);
			props.put(
					ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

			return new DefaultKafkaConsumerFactory<>(props);
		}

		@Bean
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			return factory;
		}
	}

}
