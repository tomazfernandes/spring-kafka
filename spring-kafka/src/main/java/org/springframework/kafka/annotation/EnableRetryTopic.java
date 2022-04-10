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

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaBackOffManagerConfigurationSupport;
import org.springframework.kafka.config.RetryTopicConfigurationSupport;

/**
 * Enables the non-blocking topic-based delayed retries feature. To be used in
 * {@link Configuration Configuration} classes as follows:
 * <pre class="code">
 *
 * &#064;EnableRetryTopic
 * &#064;Configuration
 * public class AppConfig {
 *
 * 	&#064;Bean
 * 	public RetryTopicConfiguration myRetryTopicConfiguration(KafkaTemplate kafkaTemplate) {
 * 		return RetryTopicConfigurationBuilder
 * 					.newInstance()
 * 					.maxAttempts(4)
 * 					.create(kafkaTemplate);
 * 	}
 * 	// other &#064;Bean definitions
 * }
 * </pre>
 *
 * To configure the feature's components, extend the {@link RetryTopicConfigurationSupport}
 * class and override the appropriate methods. Then import the subclass using the
 * {@link Import @Import} annotation on a {@link Configuration @Configuration} class,
 * such as:
 *
 * <pre class="code">
 *
 * &#064;Configuration
 * &#064;EnableKafka
 * &#064;Import(MyRetryTopicConfigurationSupport.class)
 * public class AppConfig {
 * }
 *
 * public static class MyRetryTopicConfigurationSupport extends RetryTopicConfigurationSupport {
 *
 * 		&#064;Override
 * 		protected void configureBlockingRetries(BlockingRetriesConfigurer blockingRetries) {
 * 			blockingRetries
 * 				.retryOn(ShouldRetryOnlyBlockingException.class, ShouldRetryViaBothException.class)
 * 				.backOff(new FixedBackOff(50, 3));
 * 		}
 *
 * 		&#064;Override
 * 		protected void configureNonBlockingRetries(NonBlockingRetriesConfigurer nonBlockingRetries) {
 * 			nonBlockingRetries
 * 				.addToFatalExceptions(ShouldSkipBothRetriesException.class);
 * }
 * </pre>
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Documented
@Import({RetryTopicConfigurationSupport.class, KafkaBackOffManagerConfigurationSupport.class})
public @interface EnableRetryTopic {
}
