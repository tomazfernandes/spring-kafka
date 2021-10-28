/*
 * Copyright 2021 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;

import org.springframework.beans.factory.BeanCurrentlyInCreationException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

/**
 * Handler method for retrying endpoints.
 *
 * @author Tomaz Fernandes
 * @author Gary Russell
 * @since 2.7
 *
 */
public class EndpointHandlerMethod {

	private final Class<?> beanClass;
	private String beanName;

	private Method method;

	private Object bean;
	private String methodName;

	public EndpointHandlerMethod(Class<?> beanClass, String methodName) {
		Assert.notNull(beanClass, () -> "No destination bean class provided!");
		Assert.notNull(methodName, () -> "No method name for destination bean class provided!");
		this.beanClass = beanClass;
		this.methodName = methodName;
	}

	public EndpointHandlerMethod(String beanName, String methodName) {
		Assert.notNull(beanName, () -> "No destination bean class provided!");
		Assert.notNull(methodName, () -> "No method name for destination bean class provided!");
		this.beanClass = null;
		this.beanName = beanName;
		this.methodName = methodName;
	}

	public EndpointHandlerMethod(Object bean, Method method) {
		Assert.notNull(bean, () -> "No bean for destination provided!");
		Assert.notNull(method, () -> "No method for destination bean class provided!");
		this.method = method;
		this.bean = bean;
		this.beanClass = bean.getClass();
	}

	/**
	 * Return the method.
	 * @return the method.
	 */
	public Method getMethod() {
		if (this.method != null) {
			return this.method;
		}
		Assert.state(this.bean != null, () -> "Bean should not be null at this point");
		return Arrays.stream(ReflectionUtils.getDeclaredMethods(this.bean.getClass()))
				.filter(mthd -> mthd.getName().equals(methodName))
				.findFirst()
				.orElseThrow(() -> new IllegalArgumentException(
						String.format("No method %s in class %s", methodName, this.bean.getClass())));

	}

	public Object resolveBean(BeanFactory beanFactory) {
		return resolveBean(beanFactory, null);
	}

	public Object resolveBean(BeanFactory beanFactory, Object beanCandidateFromKLME) {
		if (this.bean == null) {
			if (this.beanClass != null) {
				try {
					this.bean = beanFactory.getBean(this.beanClass);
				}
				catch (NoSuchBeanDefinitionException e) {
					String beanName = this.beanClass.getSimpleName() + "-handlerMethod";
					((BeanDefinitionRegistry) beanFactory).registerBeanDefinition(beanName,
							new RootBeanDefinition(this.beanClass));
					this.bean = beanFactory.getBean(beanName);
				}
			} else if (this.beanName != null) {
				try {
					this.bean = beanFactory.getBean(this.beanName);
				} catch (BeanCurrentlyInCreationException ex) {
					this.bean = beanCandidateFromKLME;
				}
			}
		}
		return this.bean;
	}
}
