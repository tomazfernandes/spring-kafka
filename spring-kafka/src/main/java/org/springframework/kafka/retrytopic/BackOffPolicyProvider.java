/*
 * Copyright 2018-2022 the original author or authors.
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

import org.springframework.retry.backoff.BackOffPolicy;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.backoff.ExponentialRandomBackOffPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.backoff.UniformRandomBackOffPolicy;

/**
 * Provides a {@link BackOffPolicy} from the given parameters.
 * Original logic from Spring Retry project.
 *
 * @author Tomaz Fernandes
 * @since 2.8.4
 */
class BackOffPolicyProvider {

	BackOffPolicy createBackOffPolicyFor(
			Long min, Long max, Double multiplier, Boolean isRandom) {

		if (multiplier != null && multiplier > 0) {
			ExponentialBackOffPolicy policy = new ExponentialBackOffPolicy();
			if (isRandom) {
				policy = new ExponentialRandomBackOffPolicy();
			}
			policy.setInitialInterval(min);
			policy.setMultiplier(multiplier);
			policy.setMaxInterval(max > min ? max : ExponentialBackOffPolicy.DEFAULT_MAX_INTERVAL);
			return policy;
		}
		if (max != null && min != null && max > min) {
			UniformRandomBackOffPolicy policy = new UniformRandomBackOffPolicy();
			policy.setMinBackOffPeriod(min);
			policy.setMaxBackOffPeriod(max);
			return policy;
		}
		FixedBackOffPolicy policy = new FixedBackOffPolicy();
		if (min != null) {
			policy.setBackOffPeriod(min);
		}
		return policy;
	}
}
