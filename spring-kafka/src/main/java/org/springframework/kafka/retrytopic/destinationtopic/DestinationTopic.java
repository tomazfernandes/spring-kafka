/*
 * Copyright 2018-2021 the original author or authors.
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

package org.springframework.kafka.retrytopic.destinationtopic;

/**
 *
 * This class represents a Destination Topic to which messages can be forwarded, such as retry topics and dlt.
 *
 * @author Tomaz Fernandes
 * @since 2.7.0
 * @see DestinationTopic
 *
 */
public class DestinationTopic {

	private final String destinationName;
	private final Properties properties;

	public DestinationTopic(String destinationName, Properties properties) {
		this.destinationName = destinationName;
		this.properties = properties;
	}

	public Long getDestinationDelay() {
		return this.properties.delay();
	}

	public boolean isDltTopic() {
		return this.properties.isDltTopic();
	}

	public String getDestinationName() {
		return this.destinationName;
	}

	public static final class Properties {
		private final long delayMs;
		private final String suffix;
		private final boolean isDltTopic;

		public Properties(long delayMs, String suffix, boolean isDltTopic) {
			this.delayMs = delayMs;
			this.suffix = suffix;
			this.isDltTopic = isDltTopic;
		}

		public long delay() {
			return this.delayMs;
		}

		public String suffix() {
			return this.suffix;
		}

		public boolean isDltTopic() {
			return this.isDltTopic;
		}
	}
}
