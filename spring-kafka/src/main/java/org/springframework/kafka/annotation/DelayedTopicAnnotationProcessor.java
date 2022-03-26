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

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.function.Supplier;

import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.task.TaskExecutor;
import org.springframework.kafka.config.KafkaListenerConfigUtils;
import org.springframework.kafka.listener.DelayedTopicContext;
import org.springframework.kafka.listener.KafkaConsumerBackoffManager;
import org.springframework.kafka.listener.KafkaConsumerTimingAdjuster;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.kafka.listener.PartitionPausingBackoffManager;
import org.springframework.kafka.listener.WakingKafkaConsumerTimingAdjuster;
import org.springframework.lang.Nullable;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;


/**
 * Looks for and processes a {@link DelayedTopic @DelayedTopic} annotation.
 * If such annotation is present returns the corresponding {@link DelayedTopicContext},
 * returns null otherwise.
 *
 * @author Tomaz Fernandes
 * @since 2.9
 */
public class DelayedTopicAnnotationProcessor {

	private static final String TASK_EXECUTOR_BEAN_NAME = "kafkaConsumerBackOffManagerTaskExecutor";

	private static final String THE_LEFT = "The [";

	private static final String RESOLVED_TO_LEFT = "Resolved to [";

	private static final String RIGHT_FOR_LEFT = "] for [";

	private final ApplicationContext applicationContext;

	private final BeanExpressionResolver resolver;

	private final BeanExpressionContext expressionContext;

	public DelayedTopicAnnotationProcessor(ApplicationContext applicationContext, BeanExpressionResolver resolver,
			BeanExpressionContext expressionContext) {
		this.applicationContext = applicationContext;
		this.resolver = resolver;
		this.expressionContext = expressionContext;
	}

	/**
	 * Looks up {@link DelayedTopic @DelayedTopic} annotations in the provided class
	 * and method and processes it.
	 * @param clazz the class in which to look up the annotation.
	 * @param method the method in which to look up the annotation.
	 * @return the {@link DelayedTopicContext}
	 */
	@Nullable
	public DelayedTopicContext processAnnotationFrom(Class<?> clazz, Method method) {
		DelayedTopic annotation = null;
		if (method != null) {
			annotation = AnnotatedElementUtils.findMergedAnnotation(method, DelayedTopic.class);
		}
		if (annotation == null && clazz != null) {
			annotation = AnnotatedElementUtils.findMergedAnnotation(clazz, DelayedTopic.class);
		}
		return annotation != null
				? processAnnotation(annotation)
				: null;
	}

	/**
	 * Processes a {@link DelayedTopic} annotation.
	 * @param annotation the annotation.
	 * @return the {@link DelayedTopicContext}
	 */
	public DelayedTopicContext processAnnotation(DelayedTopic annotation) {
		Duration delay = Duration.ofMillis(annotation.value());
		if (StringUtils.hasText(annotation.delayExpression())) {
			delay = resolveDelayExpression(annotation.delayExpression());
		}
		return DelayedTopicContext
				.builder()
				.delay(delay)
				.delaySource(annotation.source())
				.delayPrecision(annotation.delayPrecision())
				.producerDueTimestampHeader(annotation.producerDueTimestampHeader())
				.kafkaBackOffManager(resolveBackOffManager(annotation.kafkaConsumerBackOffManager()))
				.build();
	}

	private Duration resolveDelayExpression(String delayExpression) {
		Object resolved = resolveExpression(delayExpression);
		if (resolved instanceof Duration) {
			return (Duration) resolved;
		}
		else if (resolved instanceof Number) {
			return Duration.ofMillis(((Number) resolved).longValue());
		}
		else {
			String resolvedString = resolveExpressionAsString(delayExpression, "delayExpression");
			Supplier<String> genericErrorMessage = () -> "Could not resolve value from expression " + delayExpression;
			Assert.hasText(resolvedString, genericErrorMessage);
			try {
				return Duration.ofMillis(Long.parseLong(resolvedString));
			}
			catch (Exception e) {
				throw new IllegalArgumentException(genericErrorMessage.get());
			}
		}
	}

	private KafkaConsumerBackoffManager resolveBackOffManager(String backOffManagerName) {
		return StringUtils.hasText(backOffManagerName)
				? resolveFromBeanName(backOffManagerName)
				: resolveFromDefaultBeanNames();
	}

	private KafkaConsumerBackoffManager resolveFromBeanName(String backOffManagerName) {
		Object manager = resolveExpression(backOffManagerName);
		if (manager instanceof KafkaConsumerBackoffManager) {
			return (KafkaConsumerBackoffManager) manager;
		}
		else {
			String backOffManagerBeanName = resolveExpressionAsString(backOffManagerName, "kafkaConsumerBackOffManager");
			if (StringUtils.hasText(backOffManagerBeanName)) {
				return this.applicationContext.getBean(backOffManagerBeanName, KafkaConsumerBackoffManager.class);
			}
		}
		throw new IllegalArgumentException("Could not resolve a KafkaConsumerBackOffManager from value: " + backOffManagerName);
	}

	private KafkaConsumerBackoffManager resolveFromDefaultBeanNames() {
		if (this.applicationContext.containsBean(KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME)) {
			return this.applicationContext.getBean(KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME,
					KafkaConsumerBackoffManager.class);
		}
		else {
			Assert.isAssignable(BeanDefinitionRegistry.class, this.applicationContext.getClass(),
					"ApplicationContext needs to implement BeanDefinitionRegistry" +
							" in order to register the default back off manager.");
			ListenerContainerRegistry registry = this.applicationContext
					.getBean(KafkaListenerConfigUtils.KAFKA_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
							ListenerContainerRegistry.class);
			KafkaConsumerTimingAdjuster timingAdjuster =
					new WakingKafkaConsumerTimingAdjuster(registerAndGetTaskExecutor());
			((BeanDefinitionRegistry) this.applicationContext)
					.registerBeanDefinition(KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME,
							new RootBeanDefinition(PartitionPausingBackoffManager.class));
			return (KafkaConsumerBackoffManager) this.applicationContext
					.getBean(KafkaListenerConfigUtils.KAFKA_CONSUMER_BACK_OFF_MANAGER_BEAN_NAME, registry, timingAdjuster);
		}
	}

	private TaskExecutor registerAndGetTaskExecutor() {
		((BeanDefinitionRegistry) this.applicationContext)
				.registerBeanDefinition(TASK_EXECUTOR_BEAN_NAME, new RootBeanDefinition(ThreadPoolTaskExecutor.class));
		return this.applicationContext
				.getBean(TASK_EXECUTOR_BEAN_NAME, ThreadPoolTaskExecutor.class);
	}

	private Object resolveExpression(String value) {
		String resolved = resolve(value);
		if (this.expressionContext != null) {
			return this.resolver.evaluate(resolved, this.expressionContext);
		}
		else {
			return value;
		}
	}

	private String resolveExpressionAsString(String value, String attribute) {
		Object resolved = resolveExpression(value);
		if (resolved instanceof String && StringUtils.hasText((String) resolved)) {
			return (String) resolved;
		}
		throw new IllegalStateException(THE_LEFT + attribute + "] must resolve to a String. "
				+ RESOLVED_TO_LEFT + resolved.getClass() + RIGHT_FOR_LEFT + value + "]");
	}

	private String resolve(String value) {
		AutowireCapableBeanFactory beanFactory = this.applicationContext.getAutowireCapableBeanFactory();
		if (beanFactory instanceof ConfigurableBeanFactory) {
			return ((ConfigurableBeanFactory) beanFactory).resolveEmbeddedValue(value);
		}
		return value;
	}

}
