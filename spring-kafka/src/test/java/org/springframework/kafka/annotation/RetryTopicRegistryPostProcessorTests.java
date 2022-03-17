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

import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.retrytopic.RetryTopicBootstrapper;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicInternalBeanNames;

/**
 * @author Tomaz Fernandes
 * @since 2.8.4
 */
class RetryTopicRegistryPostProcessorTests {

	@Test
	void testWillNotBootstrapIfAlreadyHas() {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(true);
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(registry).should(never()).getBeanDefinitionNames();
	}

	@Test
	void testWillBootstrapIfHasRetryableTopicAnnotation() {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		ApplicationContext context = mock(ApplicationContext.class);
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		BeanDefinition beanDef = mock(BeanDefinition.class);
		RetryTopicBootstrapper bootstrapper = mock(RetryTopicBootstrapper.class);
		ResolvableType resolvableType = mock(ResolvableType.class);
		postProcessor.setApplicationContext(context);
		String beanName = "myBean";
		String[] beanNames = {beanName};
		given(registry.getBeanDefinitionNames()).willReturn(beanNames);
		given(registry.getBeanDefinition(beanName)).willReturn(beanDef);
		given(beanDef.getResolvableType()).willReturn(resolvableType);
		willReturn(AnnotatedRetryableTopic.class).given(resolvableType).toClass();
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(false);
		given(context.getBean(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER, RetryTopicBootstrapper.class)).willReturn(bootstrapper);
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(bootstrapper).should().bootstrapRetryTopic();
		then(registry).should().registerBeanDefinition(RetryTopicInternalBeanNames
						.RETRY_TOPIC_BOOTSTRAPPER,
				new RootBeanDefinition(RetryTopicBootstrapper.class));
	}

	@Test
	void testWillBootstrapIfHasRetryTopicConfigurationBean() {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		ApplicationContext context = mock(ApplicationContext.class);
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		BeanDefinition beanDef = mock(BeanDefinition.class);
		RetryTopicBootstrapper bootstrapper = mock(RetryTopicBootstrapper.class);
		ResolvableType resolvableType = mock(ResolvableType.class);
		postProcessor.setApplicationContext(context);
		String beanName = "myBean";
		String[] beanNames = {beanName};
		given(registry.getBeanDefinitionNames()).willReturn(beanNames);
		given(registry.getBeanDefinition(beanName)).willReturn(beanDef);
		given(beanDef.getResolvableType()).willReturn(resolvableType);
		willReturn(RetryTopicConfiguration.class).given(resolvableType).toClass();
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(false);
		given(context.getBean(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER, RetryTopicBootstrapper.class)).willReturn(bootstrapper);
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(bootstrapper).should().bootstrapRetryTopic();
		then(registry).should().registerBeanDefinition(RetryTopicInternalBeanNames
						.RETRY_TOPIC_BOOTSTRAPPER,
				new RootBeanDefinition(RetryTopicBootstrapper.class));
	}

	@Test
	void testWillNotBootstrapIfNoConfigurationFound() {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		ApplicationContext context = mock(ApplicationContext.class);
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		BeanDefinition beanDef = mock(BeanDefinition.class);
		RetryTopicBootstrapper bootstrapper = mock(RetryTopicBootstrapper.class);
		ResolvableType resolvableType = mock(ResolvableType.class);
		postProcessor.setApplicationContext(context);
		String beanName = "myBean";
		String[] beanNames = {beanName};
		given(registry.getBeanDefinitionNames()).willReturn(beanNames);
		given(registry.getBeanDefinition(beanName)).willReturn(beanDef);
		given(beanDef.getResolvableType()).willReturn(resolvableType);
		willReturn(PlainBean.class).given(resolvableType).toClass();
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(false);
		given(context.getBean(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER, RetryTopicBootstrapper.class)).willReturn(bootstrapper);
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(bootstrapper).should(never()).bootstrapRetryTopic();
		then(registry).should(never()).registerBeanDefinition(RetryTopicInternalBeanNames
						.RETRY_TOPIC_BOOTSTRAPPER,
				new RootBeanDefinition(RetryTopicBootstrapper.class));
	}

	private static class AnnotatedRetryableTopic {

		@RetryableTopic
		void myListener() {
		}
	}

	private static class PlainBean {

		void myListener() {
		}
	}
}
