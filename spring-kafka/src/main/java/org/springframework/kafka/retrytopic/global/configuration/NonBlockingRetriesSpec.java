package org.springframework.kafka.retrytopic.global.configuration;

import org.springframework.kafka.retrytopic.DefaultDestinationTopicResolver;
import org.springframework.kafka.retrytopic.RetryTopicCompositeConfigurerBuilder;
import org.springframework.util.Assert;

import java.util.Arrays;
import java.util.Collections;

public class NonBlockingRetriesSpec<P extends RetryTopicGlobalSpec>
		extends AbstractConfigurationSpec<P> {

	public NonBlockingRetriesSpec(P previousStep, RetryTopicCompositeConfigurerBuilder builder) {
		super(previousStep, builder);
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	public final NonBlockingRetriesSpec<P> addFatalTypes(Class<? extends Exception>... exceptionTypes) {
		Assert.notEmpty(exceptionTypes, "Exception types should not be null or empty");
		addConfigurer(DefaultDestinationTopicResolver.class,
				ddtr -> ddtr.addNotRetryableExceptions(exceptionTypes), () -> "adding fatal types: " + Arrays.toString(exceptionTypes));
		return this;
	}

	@SafeVarargs
	@SuppressWarnings("varargs")
	public final NonBlockingRetriesSpec<P> removeFatalTypes(Class<? extends Exception>... exceptionTypes) {
		Assert.notNull(exceptionTypes, "Exception types cannot be null");
		addConfigurer(DefaultDestinationTopicResolver.class,
				ddtr -> Arrays.stream(exceptionTypes).forEach(ddtr::removeClassification), () -> "removing fatal types: "
						+ Arrays.toString(exceptionTypes));
		return this;
	}

	public final NonBlockingRetriesSpec<P> clearAllFatalTypes() {
		addConfigurer(DefaultDestinationTopicResolver.class,
				ddtr -> ddtr.setClassifications(Collections.emptyMap(), true), () -> "clearing all fatal types");
		return this;
	}
}
