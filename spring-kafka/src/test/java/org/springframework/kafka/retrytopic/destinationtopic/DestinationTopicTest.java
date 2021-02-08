package org.springframework.kafka.retrytopic.destinationtopic;

import org.springframework.classify.BinaryExceptionClassifier;
import org.springframework.classify.BinaryExceptionClassifierBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author tomazlemos
 * @since 25/01/21
 */
public class DestinationTopicTest {

	protected KafkaOperations<Object, Object> kafkaOperations1 =
			new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Collections.emptyMap()));
	protected KafkaOperations<Object, Object> kafkaOperations2 =
			new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Collections.emptyMap()));
	private DestinationTopicPropertiesFactory.DestinationTopicSuffixes suffixes =
			new DestinationTopicPropertiesFactory.DestinationTopicSuffixes("", "");
	private String retrySuffix = suffixes.getRetrySuffix();
	private String dltSuffix = suffixes.getDltSuffix();

	private final int maxAttempts = 3;

	protected DestinationTopic.Properties mainTopicProps =
			new DestinationTopic.Properties(0, "", DestinationTopic.Type.RETRY, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ABORT, kafkaOperations1, getShouldRetryOn());
	protected DestinationTopic.Properties firstRetryProps =
			new DestinationTopic.Properties(1000, retrySuffix + "-1000", DestinationTopic.Type.RETRY, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ABORT, kafkaOperations1, getShouldRetryOn());
	protected DestinationTopic.Properties secondRetryProps =
			new DestinationTopic.Properties(2000, retrySuffix + "-2000", DestinationTopic.Type.RETRY, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ABORT, kafkaOperations1, getShouldRetryOn());
	protected DestinationTopic.Properties dltTopicProps =
			new DestinationTopic.Properties(0, dltSuffix, DestinationTopic.Type.DLT, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ABORT, kafkaOperations1, (a, e) -> false);
	protected List<DestinationTopic.Properties> allProps = Arrays
			.asList(mainTopicProps, firstRetryProps, secondRetryProps, dltTopicProps);

	protected DestinationTopic.Properties mainTopicProps2 =
			new DestinationTopic.Properties(0, "", DestinationTopic.Type.RETRY, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ALWAYS_RETRY, kafkaOperations2, getShouldRetryOn());
	protected DestinationTopic.Properties firstRetryProps2 =
			new DestinationTopic.Properties(1000, retrySuffix + "-0", DestinationTopic.Type.RETRY, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ALWAYS_RETRY, kafkaOperations2, getShouldRetryOn());
	protected DestinationTopic.Properties secondRetryProps2 =
			new DestinationTopic.Properties(1000, retrySuffix + "-1", DestinationTopic.Type.RETRY, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ALWAYS_RETRY, kafkaOperations2, getShouldRetryOn());
	protected DestinationTopic.Properties dltTopicProps2 =
			new DestinationTopic.Properties(0, dltSuffix, DestinationTopic.Type.DLT, 4, 1, RetryTopicConfiguration.DltProcessingFailureStrategy.ALWAYS_RETRY, kafkaOperations2, (a, e) -> false);
	protected List<DestinationTopic.Properties> allProps2 = Arrays
			.asList(mainTopicProps, firstRetryProps, secondRetryProps, dltTopicProps);

	protected final static String FIRST_TOPIC = "firstTopic";
	protected PropsHolder mainDestinationHolder = new PropsHolder(FIRST_TOPIC, mainTopicProps);
	protected PropsHolder firstRetryDestinationHolder = new PropsHolder(FIRST_TOPIC, firstRetryProps);
	protected PropsHolder secondRetryDestinationHolder = new PropsHolder(FIRST_TOPIC, secondRetryProps);
	protected PropsHolder dltDestinationHolder = new PropsHolder(FIRST_TOPIC, dltTopicProps);
	protected List<PropsHolder> allFirstDestinationsHolders = Arrays
			.asList(mainDestinationHolder, firstRetryDestinationHolder, secondRetryDestinationHolder, dltDestinationHolder);

	protected final static String SECOND_TOPIC = "secondTopic";
	protected PropsHolder mainDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, mainTopicProps2);
	protected PropsHolder firstRetryDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, firstRetryProps2);
	protected PropsHolder secondRetryDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, secondRetryProps2);
	protected PropsHolder dltDestinationHolder2 =
			new PropsHolder(SECOND_TOPIC, dltTopicProps2);

	protected List<PropsHolder> allSecondDestinationHolders = Arrays
			.asList(mainDestinationHolder2, firstRetryDestinationHolder2, secondRetryDestinationHolder2, dltDestinationHolder2);

	protected DestinationTopic mainDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + mainTopicProps.suffix(), mainTopicProps);
	protected DestinationTopic firstRetryDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + firstRetryProps.suffix(), firstRetryProps);
	protected DestinationTopic secondRetryDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + secondRetryProps.suffix(), secondRetryProps);
	protected DestinationTopic dltDestinationTopic =
			new DestinationTopic(FIRST_TOPIC + dltTopicProps.suffix(), dltTopicProps);
	protected DestinationTopic noOpsDestinationTopic =
			new DestinationTopic(dltDestinationTopic.getDestinationName() + "-noOps", new DestinationTopic.Properties(dltTopicProps, "-noOps", DestinationTopic.Type.NO_OPS));
	protected List<DestinationTopic> allFirstDestinationsTopics = Arrays
			.asList(mainDestinationTopic, firstRetryDestinationTopic, secondRetryDestinationTopic, dltDestinationTopic);

	protected DestinationTopic mainDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + mainTopicProps2.suffix(), mainTopicProps2);
	protected DestinationTopic firstRetryDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + firstRetryProps2.suffix(), firstRetryProps2);
	protected DestinationTopic secondRetryDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + secondRetryProps2.suffix(), secondRetryProps2);
	protected DestinationTopic dltDestinationTopic2 =
			new DestinationTopic(SECOND_TOPIC + dltTopicProps2.suffix(), dltTopicProps2);
	protected DestinationTopic noOpsDestinationTopic2 =
			new DestinationTopic(dltDestinationTopic2.getDestinationName() + "-noOps", new DestinationTopic.Properties(dltTopicProps2, "-noOps", DestinationTopic.Type.NO_OPS));

	protected List<DestinationTopic> allSecondDestinationTopics = Arrays
			.asList(mainDestinationTopic2, firstRetryDestinationTopic2, secondRetryDestinationTopic2, dltDestinationTopic2);

	private BinaryExceptionClassifier classifier = new BinaryExceptionClassifierBuilder().retryOn(IllegalArgumentException.class).build();

	private BiPredicate<Integer, Exception> getShouldRetryOn() {
		return (a, e) -> a < maxAttempts && classifier.classify(e);
	}

	class PropsHolder {
		final String topicName;
		final DestinationTopic.Properties props;

		PropsHolder(String topicName, DestinationTopic.Properties props) {
			this.topicName = topicName;
			this.props = props;
		}
	}
}
