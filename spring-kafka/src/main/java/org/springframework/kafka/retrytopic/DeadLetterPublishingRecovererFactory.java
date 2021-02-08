package org.springframework.kafka.retrytopic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.core.NestedRuntimeException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.KafkaBackoffException;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopic;
import org.springframework.kafka.retrytopic.destinationtopic.DestinationTopicResolver;
import org.springframework.util.Assert;

import java.math.BigInteger;
import java.util.stream.StreamSupport;

public class DeadLetterPublishingRecovererFactory {

	private final CharSequence KAFKA_DLT_HEADERS_PREFIX = "kafka_dlt";
	private final DestinationTopicResolver destinationTopicResolver;
	private final String NO_OPS_RETRY_TOPIC = "internal-kafka-noOpsRetry";

	public DeadLetterPublishingRecovererFactory(DestinationTopicResolver destinationTopicResolver){
		this.destinationTopicResolver = destinationTopicResolver;
	}

	public DeadLetterPublishingRecoverer create(Configuration configuration) {
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(configuration.template,
					((cr, e) -> this.resolveDestination(cr, e, configuration))) {
			@Override
			protected void publish(ProducerRecord<Object, Object> outRecord, KafkaOperations<Object, Object> kafkaTemplate) {
				if (NO_OPS_RETRY_TOPIC.equals(outRecord.topic())) {
					this.logger.warn(() -> "Processing failed for dlt topic, aborting.");
					return;
				}

				KafkaOperations<Object, Object> kafkaOperationsForTopic = (KafkaOperations<Object, Object>)
						DeadLetterPublishingRecovererFactory.this.destinationTopicResolver.getKafkaOperationsFor(outRecord.topic());
				super.publish(outRecord, kafkaOperationsForTopic);
			}
		};

		recoverer.setHeadersFunction((consumerRecord, e) -> addHeaders(consumerRecord, e, getAttempts(consumerRecord)));
		return recoverer;
	}

	private TopicPartition resolveDestination(ConsumerRecord<?, ?> cr, Exception e, Configuration configuration) {
		if (isBackoffException(e)) {
			throw (NestedRuntimeException) e; // Necessary to not commit the offset and seek to current again
		}

		int attempt = getAttempts(cr);

		DestinationTopic nextDestination = destinationTopicResolver.resolveNextDestination(cr.topic(), attempt, e);

		return nextDestination.isNoOpsTopic()
					? new TopicPartition(NO_OPS_RETRY_TOPIC, 0)
					: new TopicPartition(nextDestination.getDestinationName(),
				cr.partition() % nextDestination.getDestinationPartitions());
	}

	private boolean isBackoffException(Exception e) {
		return NestedRuntimeException.class.isAssignableFrom(e.getClass())
				&& ((NestedRuntimeException) e).contains(KafkaBackoffException.class);
	}

	private int getAttempts(ConsumerRecord<?, ?> consumerRecord) {
		Header header = consumerRecord.headers().lastHeader(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS);
		return header != null
				? header.value()[0]
				: 1;
	}

	private Headers addHeaders(ConsumerRecord<?, ?> consumerRecord, Exception e, int attempts) {
		Headers headers = filterPreviousDltHeaders(consumerRecord.headers());
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_ATTEMPTS,
				BigInteger.valueOf(attempts + 1).toByteArray());
		headers.add(RetryTopicHeaders.DEFAULT_HEADER_BACKOFF_TIMESTAMP,
					destinationTopicResolver.resolveDestinationNextExecutionTime(consumerRecord.topic(), attempts, e).getBytes());
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

	public static class Configuration {

		private final KafkaOperations<?, ?> template;

		Configuration(KafkaOperations<?, ?> template) {
			Assert.notNull(template,
					() -> "You need to provide a KafkaOperations instance.");
			this.template = template;
		}
	}
}