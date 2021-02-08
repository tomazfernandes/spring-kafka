package org.springframework.kafka.retrytopic.destinationtopic;

import java.util.ArrayList;
import java.util.Collection;
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

	private static final String NO_OPS_SUFFIX = "-noOps";
	private final DestinationTopicResolver destinationTopicResolver;

	public DefaultDestinationTopicProcessor(DestinationTopicResolver destinationTopicResolver) {
		this.destinationTopicResolver = destinationTopicResolver;
	}

	@Override
	public void processDestinationProperties(Consumer<DestinationTopic.Properties> destinationPropertiesProcessor, Context context) {
		context
				.properties
				.stream()
				.forEach(destinationPropertiesProcessor);
	}

	@Override
	public void registerDestinationTopic(String mainTopicName, String destinationTopicName,
										 DestinationTopic.Properties destinationTopicProperties, Context context) {
		List<DestinationTopic> topicDestinations = context.destinationsByTopicMap
				.computeIfAbsent(mainTopicName, newTopic -> new ArrayList<>());
		topicDestinations.add(new DestinationTopic(destinationTopicName, destinationTopicProperties));
	}

	@Override
	public void processRegisteredDestinations(Consumer<Collection<String>> topicsCallback, Context context) {
		Map<String, DestinationTopicResolver.DestinationsHolder> sourceDestinationMapForThisInstance = context.destinationsByTopicMap
				.values()
				.stream()
				.map(this::correlatePairSourceAndDestinationValues)
				.reduce(this::concatenateMaps)
				.orElseThrow(() -> new IllegalStateException("No destinations where provided for the Retry Topic configuration"));
		destinationTopicResolver.addDestinations(sourceDestinationMapForThisInstance);
		topicsCallback.accept(getAllTopicsNamesForThis(context));
	}

	private Map<String, DestinationTopicResolver.DestinationsHolder> concatenateMaps(Map<String, DestinationTopicResolver.DestinationsHolder> firstMap,
																					 Map<String, DestinationTopicResolver.DestinationsHolder> secondMap) {
		firstMap.putAll(secondMap);
		return firstMap;
	}

	private Map<String, DestinationTopicResolver.DestinationsHolder> correlatePairSourceAndDestinationValues(List<DestinationTopic> destinationList) {
		return IntStream
				.range(0, destinationList.size())
				.boxed()
				.collect(Collectors.toMap(index -> destinationList.get(index).getDestinationName(),
						index -> DestinationTopicResolver.holderFor(destinationList.get(index),
								getNextDestinationTopic(destinationList, index))));
	}

	private DestinationTopic getNextDestinationTopic(List<DestinationTopic> destinationList, int index) {
		return index != destinationList.size() - 1
				? destinationList.get(index + 1)
				: new DestinationTopic(destinationList.get(index).getDestinationName() + NO_OPS_SUFFIX,
				destinationList.get(index), NO_OPS_SUFFIX, DestinationTopic.Type.NO_OPS);
	}

	private List<String> getAllTopicsNamesForThis(Context context) {
		return context.destinationsByTopicMap
				.values()
				.stream()
				.flatMap(Collection::stream)
				.map(DestinationTopic::getDestinationName)
				.collect(Collectors.toList());
	}
}
