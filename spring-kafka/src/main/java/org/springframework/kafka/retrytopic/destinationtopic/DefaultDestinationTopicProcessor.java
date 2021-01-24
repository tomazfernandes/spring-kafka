package org.springframework.kafka.retrytopic.destinationtopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author tomazlemos
 * @since 23/01/21
 */
public class DefaultDestinationTopicProcessor implements DestinationTopicProcessor {

	private final DestinationTopicResolver destinationTopicResolver;

	public DefaultDestinationTopicProcessor(DestinationTopicResolver destinationTopicResolver) {
		this.destinationTopicResolver = destinationTopicResolver;
	}

	@Override
	public void processDestinationProperties(Consumer<DestinationTopic.Properties> destinationPropertiesProcessor, Context context) {
		context
				.properties
				.forEach(destinationPropertiesProcessor);
	}

	@Override
	public void registerTopicDestination(String mainTopic, DestinationTopic destinationTopic, Context context) {
		List<DestinationTopic> topicDestinations = context.destinationsByTopicMap.computeIfAbsent(mainTopic, this::newListWithMainTopic);
		topicDestinations.add(destinationTopic);
	}

	@Override
	public void processRegisteredDestinations(Consumer<Collection<String>> topicsConsumer, Context context) {
		Map<String, DestinationTopic> sourceDestinationMapForThisInstance = context.destinationsByTopicMap
				.values()
				.stream()
				.map(this::correlatePairSourceAndDestinationValues)
				.reduce(this::concatenateMaps)
				.orElse(Collections.emptyMap());
		//.orElseThrow(() -> new IllegalStateException("No destinations where provided for the Retry Topic configuration"));
		destinationTopicResolver.addDestinations(sourceDestinationMapForThisInstance);
		topicsConsumer.accept(getAllTopicsNames(context));
	}

	private List<DestinationTopic> newListWithMainTopic(String newTopic) {
		List<DestinationTopic> newList = new ArrayList<>();
		newList.add(new DestinationTopic(newTopic, new DestinationTopic.Properties(0, "", false)));
		return newList;
	}

	private Map<String, DestinationTopic> concatenateMaps(Map<String, DestinationTopic> firstMap, Map<String, DestinationTopic> secondMap) {
		firstMap.putAll(secondMap);
		return firstMap;
	}

	private Map<String, DestinationTopic> correlatePairSourceAndDestinationValues(List<DestinationTopic> destinationList) {
		return IntStream
				.range(0, destinationList.size() - 1)
				.boxed()
				.collect(Collectors.toMap(index -> destinationList.get(index).getDestinationName(),
						index -> getNextDestinationTopic(destinationList, index)));
	}

	private DestinationTopic getNextDestinationTopic(List<DestinationTopic> destinationList, int index) {
		return destinationList.get(index + 1);
	}

	private List<String> getAllTopicsNames(Context context) {
		return context.destinationsByTopicMap
				.values()
				.stream()
				.flatMap(Collection::stream)
				.map(DestinationTopic::getDestinationName)
				.collect(Collectors.toList());
	}
}
