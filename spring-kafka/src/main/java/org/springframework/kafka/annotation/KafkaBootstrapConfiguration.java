/*
 * Copyright 2002-2022 the original author or authors.
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

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;

/**
 * An {@link ImportBeanDefinitionRegistrar} class that registers a {@link KafkaListenerAnnotationBeanPostProcessor}
 * bean capable of processing Spring's @{@link KafkaListener} annotation and
 * a default {@link KafkaListenerEndpointRegistry}.
 *
 * Also registers a {@link RetryTopicRegistryPostProcessor} to
 * bootstrap the non-blocking delayed retries feature if such configuration is found.
 *
 * <p>This configuration class is automatically imported when using the @{@link EnableKafka}
 * annotation.  See {@link EnableKafka} Javadoc for complete usage.
 *
 * @author Stephane Nicoll
 * @author Gary Russell
 * @author Artem Bilan
 * @author Tomaz Fernandes
 *
 * @see KafkaListenerAnnotationBeanPostProcessor
 * @see KafkaListenerEndpointRegistry
 * @see RetryTopicRegistryPostProcessor
 * @see EnableKafka
 */
public class KafkaBootstrapConfiguration implements ImportBeanDefinitionRegistrar {

	@Override
	public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
		if (!registry.containsBeanDefinition(
				KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME)) {

			registry.registerBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME,
					new RootBeanDefinition(KafkaListenerAnnotationBeanPostProcessor.class));
		}

		if (!registry.containsBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME)) {
			registry.registerBeanDefinition(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
					new RootBeanDefinition(KafkaListenerEndpointRegistry.class));
		}

		if (!registry.containsBeanDefinition(KafkaListenerConfigUtils.RETRY_TOPIC_REGISTRY_POST_PROCESSOR_NAME)) {
			registry.registerBeanDefinition(KafkaListenerConfigUtils.RETRY_TOPIC_REGISTRY_POST_PROCESSOR_NAME,
					new RootBeanDefinition(RetryTopicRegistryPostProcessor.class));
		}
	}

}
