package org.springframework.kafka.retrytopic;


import java.util.function.Consumer;
import java.util.function.Supplier;

public class RetryTopicComponentConfiguration {

	public static <T> RetryTopicComponentConfigurer<T> configure(Class<T> classToConfigure, Consumer<T> configurer) {
		return RetryTopicCompositeConfigurerBuilder
				.createConfigurer(classToConfigure, configurer);
	}

	public static <T> RetryTopicComponentConfigurer<T> configure(Class<T> classToConfigure, Consumer<T> configurer,
																Supplier<String> description) {
		return RetryTopicCompositeConfigurerBuilder
				.createConfigurer(classToConfigure, configurer, description);
	}

	public static RetryTopicCompositeConfigurerBuilder configureMany() {
		return RetryTopicCompositeConfigurerBuilder.newInstance();
	}
}
