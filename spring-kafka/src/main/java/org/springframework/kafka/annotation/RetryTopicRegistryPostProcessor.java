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

import java.util.Arrays;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.Ordered;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.kafka.retrytopic.RetryTopicBootstrapper;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;

/**
 * A {@link BeanDefinitionRegistryPostProcessor} implementation that registers
 * the non-blocking delayed retries feature's components' beans if a
 * {@link RetryableTopic} annotated method or a {@link RetryTopicConfiguration}
 * bean is found.
 *
 * @author Tomaz Fernandes
 * @since 2.8.4
 *
 * @see RetryTopicBootstrapper
 * @see RetryTopicConfiguration
 * @see RetryableTopic
 */
public class RetryTopicRegistryPostProcessor
		implements BeanDefinitionRegistryPostProcessor, Ordered, ApplicationContextAware {

	private ApplicationContext applicationContext;

	@Override
	public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
		if (!registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)) {

			Arrays
					.stream(registry.getBeanDefinitionNames())
					.map(registry::getBeanDefinition)
					.filter(this::hasRetryTopicConfiguration)
					.findFirst()
					.ifPresent(beanDef -> bootstrapRetryTopic(registry));
		}
	}

	private boolean hasRetryTopicConfiguration(BeanDefinition beanDef) {
		return RetryTopicConfiguration.class.isAssignableFrom(beanDef.getResolvableType().toClass())
				|| AnnotationMetadata
					.introspect(beanDef.getResolvableType().toClass())
					.hasAnnotatedMethods(RetryableTopic.class.getName());
	}

	private void bootstrapRetryTopic(BeanDefinitionRegistry registry) {
		registry.registerBeanDefinition(RetryTopicInternalBeanNames
						.RETRY_TOPIC_BOOTSTRAPPER,
				new RootBeanDefinition(RetryTopicBootstrapper.class));
		this.applicationContext.getBean(RetryTopicInternalBeanNames
						.RETRY_TOPIC_BOOTSTRAPPER, RetryTopicBootstrapper.class)
				.bootstrapRetryTopic();
	}

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
	}

	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}
}
