package org.springframework.kafka.retrytopic;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetryableTopicAnnotationProcessorTest {

	private final String topic1 = "topic1";
	private final String topic2 = "topic2";
	private final String[] topics = {topic1, topic2};

	static final String kafkaTemplateName = "kafkaTemplateBean";
	private final String listenerMethodName = "listenWithRetry";

	@Mock
	private KafkaOperations<?, ?> kafkaOperationsFromTemplateName;

	@Mock
	private KafkaOperations<?, ?> kafkaOperationsFromDefaultName;

	@Mock
	BeanFactory beanFactory;

	Method listenWithRetryAndDlt = ReflectionUtils.findMethod(RetryableTopicAnnotationFactoryWithDlt.class, listenerMethodName);
	RetryableTopic annotationWithDlt = AnnotationUtils.findAnnotation(listenWithRetryAndDlt, RetryableTopic.class);
	Object beanWithDlt = createBean();

	Method listenWithRetry = ReflectionUtils.findMethod(RetryableTopicAnnotationFactory.class, listenerMethodName);
	RetryableTopic annotation = AnnotationUtils.findAnnotation(listenWithRetry, RetryableTopic.class);
	Object bean = createBean();

	private Object createBean() {
		try {
			return RetryableTopicAnnotationFactory.class.getDeclaredConstructor().newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Test
	void shouldGetDltHandlerMethod() {

		// setup
		when(beanFactory.getBean(RetryTopicConfigUtils.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.thenReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		assertEquals("handleDlt", dltHandlerMethod.getMethod().getName());

		assertFalse(new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure());
	}

	@Test
	void shouldGetLoggingDltHandlerMethod() {

		// setup
		when(beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).thenReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when
		RetryTopicConfiguration configuration = processor.processAnnotation(topics, listenWithRetry, annotation, bean);

		// then
		RetryTopicConfigurer.EndpointHandlerMethod dltHandlerMethod = configuration.getDltHandlerMethod();
		assertEquals(RetryTopicConfigurer.LoggingDltListenerHandlerMethod.DEFAULT_DLT_METHOD_NAME,
				dltHandlerMethod.getMethod().getName());

		assertTrue(new DestinationTopic("",
				configuration.getDestinationTopicProperties().get(0)).isAlwaysRetryOnDltFailure());
	}

	@Test
	void shouldThrowIfProvidedKafkaTemplateNotFound() {

		// setup
		when(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class)).thenThrow(NoSuchBeanDefinitionException.class);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when - then
		assertThrows(BeanInitializationException.class, () ->
				processor.processAnnotation(topics, listenWithRetry, annotation, bean));
	}

	@Test
	void shouldThrowIfNoKafkaTemplateFound() {

		// setup
		when(this.beanFactory.getBean(RetryTopicConfigUtils.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.thenThrow(NoSuchBeanDefinitionException.class);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when - then
		assertThrows(BeanInitializationException.class, () ->
				processor.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt));
	}

	@Test
	void shouldGetKafkaTemplateFromBeanName() {

		// setup
		when(this.beanFactory.getBean(this.kafkaTemplateName, KafkaOperations.class))
				.thenReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when - then
		RetryTopicConfiguration configuration = processor.processAnnotation(topics, listenWithRetry, annotation, bean);
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertEquals(kafkaOperationsFromTemplateName, destinationTopic.getKafkaOperations());
	}

	@Test
	void shouldGetKafkaTemplateFromDefaultBeanName() {

		// setup
		when(this.beanFactory.getBean(RetryTopicConfigUtils.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.thenReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		DestinationTopic.Properties properties = configuration.getDestinationTopicProperties().get(0);
		DestinationTopic destinationTopic = new DestinationTopic("", properties);
		assertEquals(kafkaOperationsFromDefaultName, destinationTopic.getKafkaOperations());
	}

	@Test
	void shouldCreateExponentialBackoff() {

		// setup
		when(this.beanFactory.getBean(RetryTopicConfigUtils.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.thenReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertEquals(0, destinationTopic.getDestinationDelay());
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertEquals(1000, destinationTopic2.getDestinationDelay());
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertEquals(2000, destinationTopic3.getDestinationDelay());
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertEquals(0, destinationTopic4.getDestinationDelay());

	}

	@Test
	void shouldSetAbort() {

		// setup
		when(this.beanFactory.getBean(RetryTopicConfigUtils.DEFAULT_KAFKA_TEMPLATE_BEAN_NAME, KafkaOperations.class))
				.thenReturn(kafkaOperationsFromDefaultName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetryAndDlt, annotationWithDlt, beanWithDlt);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertEquals(0, destinationTopic.getDestinationDelay());
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertEquals(1000, destinationTopic2.getDestinationDelay());
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertEquals(2000, destinationTopic3.getDestinationDelay());
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertEquals(0, destinationTopic4.getDestinationDelay());

	}

	@Test
	void shouldCreateFixedBackoff() {

		// setup
		when(this.beanFactory.getBean(kafkaTemplateName, KafkaOperations.class))
				.thenReturn(kafkaOperationsFromTemplateName);
		RetryableTopicAnnotationProcessor processor = new RetryableTopicAnnotationProcessor(beanFactory);

		// when
		RetryTopicConfiguration configuration = processor
				.processAnnotation(topics, listenWithRetry, annotation, bean);

		// then
		List<DestinationTopic.Properties> destinationTopicProperties = configuration.getDestinationTopicProperties();
		DestinationTopic destinationTopic = new DestinationTopic("", destinationTopicProperties.get(0));
		assertEquals(0, destinationTopic.getDestinationDelay());
		DestinationTopic destinationTopic2 = new DestinationTopic("", destinationTopicProperties.get(1));
		assertEquals(1000, destinationTopic2.getDestinationDelay());
		DestinationTopic destinationTopic3 = new DestinationTopic("", destinationTopicProperties.get(2));
		assertEquals(1000, destinationTopic3.getDestinationDelay());
		DestinationTopic destinationTopic4 = new DestinationTopic("", destinationTopicProperties.get(3));
		assertEquals(0, destinationTopic4.getDestinationDelay());

	}

	static class RetryableTopicAnnotationFactory {

		@KafkaListener
		@RetryableTopic(kafkaTemplate = RetryableTopicAnnotationProcessorTest.kafkaTemplateName)
		void listenWithRetry() {
			// NoOps
		}
	}

	static class RetryableTopicAnnotationFactoryWithDlt {

		@KafkaListener
		@RetryableTopic(attempts = 3, backoff = @Backoff(multiplier = 2, value = 1000),
			dltProcessingFailureStrategy = RetryTopicConfiguration.DltProcessingFailureStrategy.ABORT)
		void listenWithRetry() {
			// NoOps
		}

		@DltHandler
		void handleDlt() {
			// NoOps
		}
	}
}