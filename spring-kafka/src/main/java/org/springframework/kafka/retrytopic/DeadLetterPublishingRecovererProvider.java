package org.springframework.kafka.retrytopic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.core.NestedRuntimeException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.listener.adapter.KafkaBackoffAwareMessageListenerAdapter;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicContainer;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicResolver;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.Map;
import java.util.stream.StreamSupport;

public class DeadLetterPublishingRecovererProvider {

	private final CharSequence KAFKA_DLT_HEADERS_PREFIX = "kafka_dlt";
	private final BinaryExceptionClassifier NO_CLASSIFIER = null;
	private final DestinationTopicResolver destinationTopicResolver;

	public DeadLetterPublishingRecovererProvider(DestinationTopicResolver destinationTopicResolver){
		this.destinationTopicResolver = destinationTopicResolver;
	}

	public DeadLetterPublishingRecoverer create(Configuration configuration) {
		DeadLetterPublishingRecoverer recoverer = configuration.templates.size() == 1
				? new DeadLetterPublishingRecoverer(configuration.templates.entrySet().iterator().next().getValue(), ((cr, e) -> this.resolveDestination(cr, e, this.destinationTopicResolver, configuration)))
				: new DeadLetterPublishingRecoverer(configuration.templates, (cr, e) -> resolveDestination(cr, e, this.destinationTopicResolver, configuration));

		recoverer.setHeadersFunction((consumerRecord, e) -> addBackoffHeader(consumerRecord, e, this.destinationTopicResolver, configuration));
		return recoverer;
	}

	private TopicPartition resolveDestination(ConsumerRecord<?, ?> cr, Exception e, DestinationTopicResolver destinationTopicResolver, Configuration configuration) {
		if (isBackoffException(e)) {
			throw (NestedRuntimeException) e; // Necessary to not commit the offset and seek to current again
		}

		// TODO: Handle the cases where the destination topics has less than the main topic number of partitions
		return isRetryableException(e, configuration)
				? new TopicPartition(destinationTopicResolver.resolveDestinationFor(cr.topic()), cr.partition())
				: handleDltTopicPartition(cr, destinationTopicResolver);
	}

	private boolean isRetryableException(Exception e, Configuration configuration) {
		return configuration.exceptionClassifier == this.NO_CLASSIFIER
				|| !configuration.exceptionClassifier.classify(e);
	}

	private TopicPartition handleDltTopicPartition(ConsumerRecord<?, ?> cr, DestinationTopicResolver destinationTopicResolver) {
		return new TopicPartition(destinationTopicResolver.resolveDltDestinationFor(cr.topic()), cr.partition());
	}

	private boolean isBackoffException(Exception e) {
		return NestedRuntimeException.class.isAssignableFrom(e.getClass())
				&& ((NestedRuntimeException) e).contains(KafkaBackoffException.class);
	}

	private Headers addBackoffHeader(ConsumerRecord<?, ?> consumerRecord, Exception e, DestinationTopicResolver destinationTopicResolver, Configuration configuration) {
		Headers headers = filterPreviousDltHeaders(consumerRecord.headers());
		if (isRetryableException(e, configuration)) {
			headers.add(KafkaBackoffAwareMessageListenerAdapter.DEFAULT_HEADER_BACKOFF_TIMESTAMP, destinationTopicResolver.resolveDestinationNextExecutionTime(consumerRecord.topic()).getBytes());
		}
		return headers;
	}

	// TODO: Integrate better with the DeadLetterPublisherRecoverer so that the headers end up better.
	private RecordHeaders filterPreviousDltHeaders(Headers headers) {
		return StreamSupport
				.stream(headers.spliterator(), false)
				.filter(header -> !new String(header.value()).contains(this.KAFKA_DLT_HEADERS_PREFIX))
				.reduce(new RecordHeaders(), ((recordHeaders, header) -> {
					recordHeaders.add(header);
					return recordHeaders;
				}), (a, b) -> a);
	}

	static class Configuration {
		private final Map<Class<?>, KafkaOperations<?, ?>> templates;
		private final BinaryExceptionClassifier exceptionClassifier;

		Configuration(Map<Class<?>, KafkaOperations<?, ?>> templates, BinaryExceptionClassifier exceptionClassifier) {
			Assert.isTrue(templates != null && !templates.isEmpty(),
					() -> "At least one template is necessary.");
			this.templates = templates;
			this.exceptionClassifier = exceptionClassifier;
		}

		public BinaryExceptionClassifier getExceptionClassifier() {
			return exceptionClassifier;
		}

		public Map<Class<?>, KafkaOperations<?, ?>> getTemplates() {
			return templates;
		}
	}
}