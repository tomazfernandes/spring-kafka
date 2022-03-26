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

import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.kafka.listener.DelayedTopicContext;
import org.springframework.lang.Nullable;

/**
 * Provides a {@link DelayedTopicContext} for the given
 * {@link MethodKafkaListenerEndpoint}, if there's any.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public class DelayedTopicPropertiesProvider {

	private final DelayedTopicAnnotationProcessor processor;

	public DelayedTopicPropertiesProvider(ApplicationContext applicationContext, BeanExpressionResolver resolver,
			BeanExpressionContext expressionContext) {
		this.processor = createProcessor(applicationContext, resolver, expressionContext);
	}

	/**
	 * Provide a {@link DelayedTopicContext} for a {@link MethodKafkaListenerEndpoint}.
	 * @param endpoint the endpoint to provide a {@link DelayedTopicContext} for.
	 * @return the {@link DelayedTopicContext}.
	 */
	@Nullable
	public DelayedTopicContext getContextFor(MethodKafkaListenerEndpoint<?, ?> endpoint) {
		return this.processor.processAnnotationFrom(AopUtils.getTargetClass(endpoint.getBean()), endpoint.getMethod());
	}

	protected DelayedTopicAnnotationProcessor createProcessor(ApplicationContext beanFactory,
															BeanExpressionResolver resolver,
															BeanExpressionContext expressionContext) {
		return new DelayedTopicAnnotationProcessor(beanFactory, resolver, expressionContext);
	}

}
