/*
 * Copyright 2022-2022 the original author or authors.
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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.retrytopic.DestinationTopicResolver;
import org.springframework.kafka.retrytopic.RetryTopicBootstrapper;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * @author Tomaz Fernandes
 * @since 2.9
 */
class RetryTopicRegistryPostProcessorIntegrationTests {

	@SpringJUnitConfig
	@DirtiesContext
	@EmbeddedKafka
	static class RetryableTopicAnnotationTests {

		@Test
		void contextLoads() {
		}

		static class AnnotatedListener {

			@Autowired
			DestinationTopicResolver resolver;

			@RetryableTopic
			@KafkaListener(topics = "myTopic")
			void listen() {

			}
		}

		@EnableKafka
		@Configuration
		@Import(CommonConfiguration.class)
		static class TestConfiguration {

			@Bean
			AnnotatedListener annotatedListener() {
				return new AnnotatedListener();
			}
		}
	}

	@SpringJUnitConfig
	@DirtiesContext
	@EmbeddedKafka
	static class RetryTopicConfigurationBeanTests {

		@Test
		void contextLoads() {
		}

		static class PlainListener {

			@Autowired
			DestinationTopicResolver resolver;

			@KafkaListener(topics = "myTopic")
			void listen() {

			}
		}

		@EnableKafka
		@Configuration
		@Import(CommonConfiguration.class)
		static class TestConfiguration {

			@Autowired
			KafkaTemplate<?, ?> kafkaTemplate;

			@Bean
			PlainListener annotatedListener() {
				return new PlainListener();
			}

			@Bean
			RetryTopicConfiguration retryTopicConfiguration() {
				return RetryTopicConfigurationBuilder.newInstance().create(kafkaTemplate);
			}
		}
	}

	@SpringJUnitConfig
	@DirtiesContext
	@EmbeddedKafka
	static class DisabledEarlyConfigurationTests {

		@Autowired
		PlainListener plainListener;

		@Autowired
		ApplicationContext context;

		@Test
		void contextLoads() {
			assertThat(plainListener.resolver).isNull();
			assertThat(context.getBean(RetryTopicInternalBeanNames.DESTINATION_TOPIC_CONTAINER_NAME,
					DestinationTopicResolver.class)).isNotNull();
		}

		static class PlainListener {

			@Autowired(required = false)
			DestinationTopicResolver resolver;

			@KafkaListener(topics = "myTopic")
			void listen() {

			}
		}

		@EnableKafka
		@Configuration
		@Import(CommonConfiguration.class)
		static class TestConfiguration {

			@Autowired
			KafkaTemplate<?, ?> kafkaTemplate;

			@Bean
			PlainListener annotatedListener() {
				return new PlainListener();
			}

			@Bean
			RetryTopicConfiguration retryTopicConfiguration() {
				return RetryTopicConfigurationBuilder.newInstance().create(kafkaTemplate);
			}

			@Bean(name = KafkaListenerConfigUtils.RETRY_TOPIC_REGISTRY_POST_PROCESSOR_NAME)
			RetryTopicRegistryPostProcessor retryTopicRegistryPostProcessor() {
				return new RetryTopicRegistryPostProcessor() {
					@Override
					public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
						// No-ops
					}
				};
			}
		}
	}

	@SpringJUnitConfig
	@DirtiesContext
	@EmbeddedKafka
	static class UseRegisteredBootstrapperTests {

		@Autowired
		LatchContainer latchContainer;

		@Test
		void contextLoads() throws InterruptedException {
			assertThat(latchContainer.countDownLatch.await(30, TimeUnit.SECONDS)).isTrue();
		}

		static class PlainListener {

			@Autowired
			DestinationTopicResolver resolver;

			@KafkaListener(topics = "myTopic")
			void listen() {

			}
		}

		static class LatchContainer {
			CountDownLatch countDownLatch = new CountDownLatch(1);
		}

		@EnableKafka
		@Configuration
		@Import(CommonConfiguration.class)
		static class TestConfiguration {

			@Autowired
			KafkaTemplate<?, ?> kafkaTemplate;

			@Bean
			PlainListener annotatedListener() {
				return new PlainListener();
			}

			@Bean
			LatchContainer latchContainer() {
				return new LatchContainer();
			}

			@Bean(name = RetryTopicInternalBeanNames.RETRY_TOPIC_BOOTSTRAPPER)
			RetryTopicBootstrapper retryTopicBootstrapper(LatchContainer latchContainer) {
				return new RetryTopicBootstrapper() {

					@Override
					public void bootstrapRetryTopic() {
						super.bootstrapRetryTopic();
						latchContainer.countDownLatch.countDown();
					}
				};
			}

			@Bean
			RetryTopicConfiguration retryTopicConfiguration() {
				return RetryTopicConfigurationBuilder.newInstance().create(kafkaTemplate);
			}
		}
	}

	@SpringJUnitConfig
	@DirtiesContext
	@EmbeddedKafka
	static class NoConfigurationTests {

		@Autowired
		PlainListener plainListener;

		@Test
		void contextLoadsWithoutBootstrapping() {
			assertThat(plainListener.resolver).isNull();
		}

		static class PlainListener {

			@Autowired(required = false)
			DestinationTopicResolver resolver;

			@KafkaListener(topics = "myTopic")
			void listen() {

			}
		}

		@EnableKafka
		@Configuration
		@Import(CommonConfiguration.class)
		static class TestConfiguration {

			@Bean
			PlainListener annotatedListener() {
				return new PlainListener();
			}
		}
	}

	@Configuration
	static class CommonConfiguration {

		@Autowired
		private EmbeddedKafkaBroker broker;

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
		public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
				ConsumerFactory<String, String> consumerFactory) {

			ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
			factory.setConsumerFactory(consumerFactory);
			factory.setConcurrency(1);
			return factory;
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
		public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
			return new KafkaTemplate<>(producerFactory);
		}
	}
}
