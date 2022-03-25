package org.springframework.kafka.retrytopic.global.configuration;

import org.springframework.kafka.retrytopic.RetryTopicComponentConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicCompositeConfigurer;
import org.springframework.kafka.retrytopic.RetryTopicCompositeConfigurerBuilder;

public class RetryTopicGlobalConfiguration implements RetryTopicGlobalSpec {

	private final RetryTopicCompositeConfigurerBuilder builder;
	private final CustomizersSpec<RetryTopicGlobalConfiguration> customizers;
	private final BlockingRetriesSpec<RetryTopicGlobalConfiguration> blockingRetries;
	private final NonBlockingRetriesSpec<RetryTopicGlobalConfiguration> nonBlockingRetries;

	public BlockingRetriesSpec<RetryTopicGlobalConfiguration> blockingRetries() {
		return this.blockingRetries;
	}

	public NonBlockingRetriesSpec<RetryTopicGlobalConfiguration> nonBlockingRetries() {
		return this.nonBlockingRetries;
	}

	public static RetryTopicGlobalConfiguration configure() {
		return new RetryTopicGlobalConfiguration();
	}

	private RetryTopicGlobalConfiguration(){
		this.builder = RetryTopicComponentConfiguration.configureMany();
		this.customizers = new CustomizersSpec<>(this, this.builder);
		this.blockingRetries = new BlockingRetriesSpec<>(this, this.builder);
		this.nonBlockingRetries = new NonBlockingRetriesSpec<>(this, this.builder);
	}

	public CustomizersSpec<RetryTopicGlobalConfiguration> customizers() {
		return this.customizers;
	}

	public RetryTopicCompositeConfigurer done() {
		return this.builder.done();
	}
}
