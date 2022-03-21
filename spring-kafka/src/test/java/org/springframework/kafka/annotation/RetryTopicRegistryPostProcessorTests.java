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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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
 * @since 2.9.0
 */
class RetryTopicRegistryPostProcessorTests {

	@Test
	void testWillNotBootstrapIfAlreadyHas() {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(true);
		BeanDefinition beanDef = mock(BeanDefinition.class);
		given(registry.getBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(beanDef);
		given(beanDef.hasAttribute(RetryTopicRegistryPostProcessor.RETRY_TOPIC_BOOTSTRAPPED_ATTRIBUTE))
				.willReturn(true);
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(registry).should(never()).getBeanDefinitionNames();
	}

	@Test
	void testWillBootstrapIfHasRetryTopicConfigurationBean() {
		testBootstrappingFor(RetryTopicConfiguration.class);
	}

	@Test
	void testWillBootstrapIfHasRetryableTopicAnnotation() {
		testBootstrappingFor(AnnotatedRetryableTopic.class);
	}

	private void testBootstrappingFor(Class<?> beanClass) {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		ApplicationContext context = mock(ApplicationContext.class);
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		BeanDefinition beanDef = mock(BeanDefinition.class);
		BeanDefinition bootstrapperBeanDef = mock(BeanDefinition.class);
		RetryTopicBootstrapper bootstrapper = mock(RetryTopicBootstrapper.class);
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(false);
		ResolvableType resolvableType = mock(ResolvableType.class);
		postProcessor.setApplicationContext(context);
		String beanName = "myBean";
		String[] beanNames = {beanName};
		given(registry.getBeanDefinitionNames()).willReturn(beanNames);
		given(registry.getBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(bootstrapperBeanDef);
		given(registry.getBeanDefinition(beanName)).willReturn(beanDef);
		given(beanDef.getResolvableType()).willReturn(resolvableType);
		willReturn(beanClass).given(resolvableType).toClass();
		given(context.getBean(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER, RetryTopicBootstrapper.class)).willReturn(bootstrapper);
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(bootstrapper).should().bootstrapRetryTopic();
		ArgumentCaptor<RootBeanDefinition> captor = ArgumentCaptor.forClass(RootBeanDefinition.class);
		then(registry).should().registerBeanDefinition(eq(RetryTopicInternalBeanNames
						.RETRY_TOPIC_BOOTSTRAPPER),
				captor.capture());
		RootBeanDefinition value = captor.getValue();
		assertThat(value.getAttribute(RetryTopicRegistryPostProcessor.RETRY_TOPIC_BOOTSTRAPPED_ATTRIBUTE))
				.isEqualTo(Boolean.TRUE);
	}

	@Test
	void testWillNotBootstrapIfNoConfigurationFound() {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		ApplicationContext context = mock(ApplicationContext.class);
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		BeanDefinition beanDef = mock(BeanDefinition.class);
		BeanDefinition bootstrapperBeanDef = mock(BeanDefinition.class);
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(false);
		ResolvableType resolvableType = mock(ResolvableType.class);
		postProcessor.setApplicationContext(context);
		String beanName = "myBean";
		String[] beanNames = {beanName};
		given(registry.getBeanDefinitionNames()).willReturn(beanNames);
		given(registry.getBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(bootstrapperBeanDef);
		given(registry.getBeanDefinition(beanName)).willReturn(beanDef);
		given(beanDef.getResolvableType()).willReturn(resolvableType);
		willReturn(PlainBean.class).given(resolvableType).toClass();
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(context).should(never()).getBean(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER, RetryTopicBootstrapper.class);
	}

	@Test
	void testWillUseExistingRetryTopicBootstrapperBeanDefinition() {
		RetryTopicRegistryPostProcessor postProcessor = new RetryTopicRegistryPostProcessor();
		ApplicationContext context = mock(ApplicationContext.class);
		BeanDefinitionRegistry registry = mock(BeanDefinitionRegistry.class);
		BeanDefinition bootsTrapperBeanDef = mock(BeanDefinition.class);
		BeanDefinition beanDef = mock(BeanDefinition.class);
		RetryTopicBootstrapper bootstrapper = mock(RetryTopicBootstrapper.class);
		given(registry.containsBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(true);
		given(bootsTrapperBeanDef.hasAttribute(RetryTopicRegistryPostProcessor.RETRY_TOPIC_BOOTSTRAPPED_ATTRIBUTE))
				.willReturn(false);
		ResolvableType resolvableType = mock(ResolvableType.class);
		postProcessor.setApplicationContext(context);
		String beanName = "myBean";
		String[] beanNames = {beanName};
		given(registry.getBeanDefinitionNames()).willReturn(beanNames);
		given(registry.getBeanDefinition(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER)).willReturn(bootsTrapperBeanDef);
		given(registry.getBeanDefinition(beanName)).willReturn(beanDef);
		given(beanDef.getResolvableType()).willReturn(resolvableType);
		willReturn(AnnotatedRetryableTopic.class).given(resolvableType).toClass();
		given(context.getBean(RetryTopicInternalBeanNames
				.RETRY_TOPIC_BOOTSTRAPPER, RetryTopicBootstrapper.class)).willReturn(bootstrapper);
		postProcessor.postProcessBeanDefinitionRegistry(registry);
		then(bootstrapper).should().bootstrapRetryTopic();
		then(registry).should(never()).registerBeanDefinition(eq(RetryTopicInternalBeanNames
						.RETRY_TOPIC_BOOTSTRAPPER),
				any(RootBeanDefinition.class));
		then(bootsTrapperBeanDef).should()
				.setAttribute(RetryTopicRegistryPostProcessor.RETRY_TOPIC_BOOTSTRAPPED_ATTRIBUTE, true);
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
