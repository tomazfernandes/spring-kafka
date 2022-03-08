/*
 * Copyright 2021-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.retrytopic;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;

/**
 * @author Tomaz Fernandes
 * @since 2.8.4
 */
class BackOffPolicyProviderTest {

	private final BackOffPolicyProvider provider = new BackOffPolicyProvider();

	@Test
	void shouldCreateExponentialBackOff() {
		BackOffPolicy backOffPolicy = provider.createBackOffPolicyFor(100L, 1000L, 2.0, false);
		assertThat(backOffPolicy).isInstanceOf(ExponentialBackOffPolicy.class);
		ExponentialBackOffPolicy exponentialBackOffPolicy = (ExponentialBackOffPolicy) backOffPolicy;
		assertThat(exponentialBackOffPolicy.getInitialInterval()).isEqualTo(100);
		assertThat(exponentialBackOffPolicy.getMaxInterval()).isEqualTo(1000);
		assertThat(exponentialBackOffPolicy.getMultiplier()).isEqualTo(2.0);
	}

	@Test
	void shouldCreateExponentialRandomBackOff() {
		BackOffPolicy backOffPolicy = provider.createBackOffPolicyFor(10000L, 100000L, 10.0, true);
		assertThat(backOffPolicy).isInstanceOf(ExponentialRandomBackOffPolicy.class);
		ExponentialBackOffPolicy exponentialRandomBackOffPolicy = (ExponentialBackOffPolicy) backOffPolicy;
		assertThat(exponentialRandomBackOffPolicy.getInitialInterval()).isEqualTo(10000);
		assertThat(exponentialRandomBackOffPolicy.getMaxInterval()).isEqualTo(100000L);
		assertThat(exponentialRandomBackOffPolicy.getMultiplier()).isEqualTo(10.0);
	}

	@Test
	void shouldCreateUniformRandomBackOffPolicy() {
		BackOffPolicy backOffPolicy = provider.createBackOffPolicyFor(1L, 5000L, null, null);
		assertThat(backOffPolicy).isInstanceOf(UniformRandomBackOffPolicy.class);
		UniformRandomBackOffPolicy uniformRandomBackOffPolicy = (UniformRandomBackOffPolicy) backOffPolicy;
		assertThat(uniformRandomBackOffPolicy.getMinBackOffPeriod()).isEqualTo(1);
		assertThat(uniformRandomBackOffPolicy.getMaxBackOffPeriod()).isEqualTo(5000L);
	}

	@Test
	void shouldCreateFixedBackOffPolicy() {
		BackOffPolicy backOffPolicy = provider.createBackOffPolicyFor(5000L, null, null, null);
		assertThat(backOffPolicy).isInstanceOf(FixedBackOffPolicy.class);
		FixedBackOffPolicy fixedBackOffPolicy = (FixedBackOffPolicy) backOffPolicy;
		assertThat(fixedBackOffPolicy.getBackOffPeriod()).isEqualTo(5000L);
	}
}
