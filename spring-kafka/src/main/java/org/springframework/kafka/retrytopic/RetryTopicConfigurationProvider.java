package org.springframework.kafka.retrytopic;

import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.kafka.annotation.RetryableTopic;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * @author tomazlemos
 * @since 24/01/21
 */
public class RetryTopicConfigurationProvider {

	private final BeanFactory beanFactory;
	private static final LogAccessor logger = new LogAccessor(LogFactory.getLog(RetryTopicConfigurationProvider.class));

	public RetryTopicConfigurationProvider(BeanFactory beanFactory) {
		this.beanFactory = beanFactory;
	}
	public RetryTopicConfiguration findRetryConfigurationFor(String[] topics, Method method, Object bean) {
		// TODO: Enable class-level annotations
		RetryableTopic annotation = AnnotationUtils.findAnnotation(method, RetryableTopic.class);
		return annotation != null
				? new RetryableTopicAnnotationProcessor(this.beanFactory)
				.processAnnotation(topics, method, annotation, bean)
				: maybeGetFromContext(topics);
	}

	private RetryTopicConfiguration maybeGetFromContext(String[] topics) {
		if (this.beanFactory == null || !ListableBeanFactory.class.isAssignableFrom(this.beanFactory.getClass())) {
			logger.warn("No ListableBeanFactory found, skipping RetryTopic configuration.");
			return null;
		}

		Map<String, RetryTopicConfiguration> retryTopicProcessors = ((ListableBeanFactory) this.beanFactory)
				.getBeansOfType(RetryTopicConfiguration.class);
		return retryTopicProcessors
				.values()
				.stream()
				.filter(topicConfiguration -> topicConfiguration.hasConfigurationForTopics(topics))
				.findFirst()
				.orElse(null);
	}
}
