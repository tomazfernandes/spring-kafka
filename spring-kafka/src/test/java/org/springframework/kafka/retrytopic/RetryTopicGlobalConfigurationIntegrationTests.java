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

package org.springframework.kafka.retrytopic;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.retrytopic.global.configuration.RetryTopicGlobalConfiguration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.converter.ConversionException;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;


/**
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 */
@SpringJUnitConfig
@DirtiesContext
@EmbeddedKafka(topics = { RetryTopicGlobalConfigurationIntegrationTests.FIRST_TOPIC,
		RetryTopicGlobalConfigurationIntegrationTests.SECOND_TOPIC,
		RetryTopicGlobalConfigurationIntegrationTests.THIRD_TOPIC,
		RetryTopicGlobalConfigurationIntegrationTests.FOURTH_TOPIC }, partitions = 1)
@TestPropertySource(properties = "five.attempts=5")
public class RetryTopicGlobalConfigurationIntegrationTests {

	private static final Logger logger = LoggerFactory.getLogger(RetryTopicGlobalConfigurationIntegrationTests.class);

	public final static String FIRST_TOPIC = "myRetryTopic1";

	public final static String SECOND_TOPIC = "myRetryTopic2";

	public final static String THIRD_TOPIC = "myRetryTopic3";

	public final static String FOURTH_TOPIC = "myRetryTopic4";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private CountDownLatchContainer latchContainer;

	@Test
	void shouldRetryFirstTopic() {
		logger.debug("Sending message to topic " + FIRST_TOPIC);
		kafkaTemplate.send(FIRST_TOPIC, "Testing topic 1");
		assertThat(awaitLatch(latchContainer.countDownLatch1)).isTrue();
		assertThat(awaitLatch(latchContainer.customDltCountdownLatch)).isTrue();
	}

	private boolean awaitLatch(CountDownLatch latch) {
		try {
			return latch.await(60, TimeUnit.SECONDS);
		}
		catch (Exception e) {
			fail(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	@Component
	static class FirstTopicListener {

		@Autowired
		ApplicationContext context;

		RetryTopicComponents components;

		@Autowired
		CountDownLatchContainer container;

		@KafkaListener(id = "firstTopicId", topics = FIRST_TOPIC)
		public void listen(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic) {
			if (components == null) {
				components = context.getBean(RetryTopicComponents.class);
			}
			DefaultDestinationTopicProcessor ddtr = components
					.getComponentInstance(RetryTopicComponents.defaultDestinationTopicProcessor());
			logger.debug("Message {} received in topic {}", message, receivedTopic);
				container.countDownLatch1.countDown();
			throw new RuntimeException("Woooops... in topic " + receivedTopic);
		}
	}

	@Component
	static class CountDownLatchContainer {

		CountDownLatch countDownLatch1 = new CountDownLatch(5);
		CountDownLatch customDltCountdownLatch = new CountDownLatch(1);

	}

	@Component
	static class MyCustomDltProcessor {

		@Autowired
		KafkaTemplate<String, String> kafkaTemplate;

		@Autowired
		CountDownLatchContainer container;

		public void processDltMessage(Object message) {
			container.customDltCountdownLatch.countDown();
			logger.info("Received message in custom dlt!");
			logger.info("Just showing I have an injected kafkaTemplate! " + kafkaTemplate);
			throw new RuntimeException("Dlt Error!");
		}
	}

	@Configuration
	static class RetryTopicConfigurations {

		private static final String DLT_METHOD_NAME = "processDltMessage";

		@Bean
		public RetryTopicComponentConfigurer<DefaultDestinationTopicResolver> ddtrCustomizer() {
			return RetryTopicComponentConfiguration
					.configure(RetryTopicComponents.defaultDestinationTopicResolver(),
							ddtr -> ddtr.setLogLevel(KafkaException.Level.DEBUG), () -> "Setting log level to DEBUG");
		}

		@Bean
		public RetryTopicCompositeConfigurer configureMultipleComponents() {
			return RetryTopicComponentConfiguration
					.configureMany()
						.configure(RetryTopicComponents.defaultDestinationTopicResolver(),
								ddtr -> ddtr.setLogLevel(KafkaException.Level.DEBUG),
								() -> "Setting log level to DEBUG")
						.configure(DeadLetterPublishingRecovererFactory.class,
								dlprf -> dlprf.removeNotRetryableException(ConversionException.class))
					.done();
		}

		@Bean
		public RetryTopicCompositeConfigurer globalConfiguration() {
			return RetryTopicGlobalConfiguration
					.configure()
					.customizers()
						.commonErrorHandler(eh -> eh.setAckAfterHandle(true))
						.deadLetterPublishingRecoverer(recoverer -> recoverer.setAppendOriginalHeaders(true))
						.listenerContainer(container -> container.getContainerProperties().setPollTimeout(1500))
					.and()
					.blockingRetries()
						.retryOnExceptionTypes(IllegalStateException.class, IllegalArgumentException.class)
						.backOffPolicy(new FixedBackOff(2000, 5))
					.and()
					.nonBlockingRetries()
						.clearAllFatalTypes()
						.addFatalTypes(IllegalArgumentException.class)
						.removeFatalTypes(ConversionException.class)
						.and()
					.done();
		}

		@Bean
		public RetryTopicConfiguration firstRetryTopic(KafkaTemplate<String, String> template) {
			return RetryTopicConfigurationBuilder
					.newInstance()
					.fixedBackOff(50)
					.maxAttempts(5)
					.useSingleTopicForFixedDelays()
					.includeTopic(FIRST_TOPIC)
					.doNotRetryOnDltFailure()
					.dltHandlerMethod("myCustomDltProcessor", DLT_METHOD_NAME)
					.create(template);
		}

		@Bean
		public FirstTopicListener firstTopicListener() {
			return new FirstTopicListener();
		}

		@Bean
		CountDownLatchContainer latchContainer() {
			return new CountDownLatchContainer();
		}

		@Bean
		MyCustomDltProcessor myCustomDltProcessor() {
			return new MyCustomDltProcessor();
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
			ContainerProperties props = factory.getContainerProperties();
			props.setIdleEventInterval(100L);
			props.setPollTimeout(50L);
			props.setIdlePartitionEventInterval(100L);
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			factory.setContainerCustomizer(
					container -> container.getContainerProperties().setIdlePartitionEventInterval(100L));
			return factory;
		}
	}

}
