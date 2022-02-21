/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.kafka;

import static org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementation.LEGACY;
import static org.apache.beam.sdk.io.kafka.KafkaIOReadImplementationCompatibility.KafkaIOReadImplementation.SDF;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.HashMultimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Multimap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;

class KafkaIOReadImplementationCompatibility {

  enum KafkaIOReadImplementation {
    LEGACY,
    SDF
  }

  @VisibleForTesting
  @SuppressWarnings("ImmutableEnumChecker")
  enum KafkaIOReadProperties {
    CONSUMER_CONFIG,
    TOPICS,
    TOPIC_PARTITIONS,
    KEY_CODER,
    VALUE_CODER,
    CONSUMER_FACTORY_FN,
    WATERMARK_FN(LEGACY),
    MAX_NUM_RECORDS(LEGACY) {
      @Override
      Object getDefaultValue() {
        return Long.MAX_VALUE;
      }
    },
    MAX_READ_TIME(LEGACY),
    START_READ_TIME,
    STOP_READ_TIME(SDF),
    COMMIT_OFFSETS_IN_FINALIZE_ENABLED {
      @Override
      Object getDefaultValue() {
        return false;
      }
    },
    DYNAMIC_READ(SDF) {
      @Override
      Object getDefaultValue() {
        return false;
      }
    },
    WATCH_TOPIC_PARTITION_DURATION(SDF),
    TIMESTAMP_POLICY_FACTORY,
    OFFSET_CONSUMER_CONFIG,
    KEY_DESERIALIZER_PROVIDER,
    VALUE_DESERIALIZER_PROVIDER,
    CHECK_STOP_READING_FN(SDF),
    ;

    @Nonnull private final ImmutableSet<KafkaIOReadImplementation> supportedImplementations;
    @Nonnull private final Method getterMethod;

    private KafkaIOReadProperties() {
      this(KafkaIOReadImplementation.values());
    }

    private KafkaIOReadProperties(@Nonnull KafkaIOReadImplementation... supportedImplementations) {
      this.supportedImplementations =
          Sets.immutableEnumSet(Arrays.asList(supportedImplementations));
      this.getterMethod = findGetterMethod(this);
    }

    private Method findGetterMethod(KafkaIOReadProperties property) {
      final String propertyNameInUpperCamel =
          CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, property.name());
      try {
        return KafkaIO.Read.class.getDeclaredMethod("get" + propertyNameInUpperCamel);
      } catch (NoSuchMethodException e) {
        try {
          return KafkaIO.Read.class.getDeclaredMethod("is" + propertyNameInUpperCamel);
        } catch (NoSuchMethodException e2) {
          throw new RuntimeException("Should not happen", e);
        }
      }
    }

    @VisibleForTesting
    Object getDefaultValue() {
      return null;
    }

    @VisibleForTesting
    Method getGetterMethod() {
      return getterMethod;
    }
  }

  static KafkaIOReadImplementationCompatibilityResult getCompatibility(KafkaIO.Read<?, ?> read) {
    final Multimap<KafkaIOReadImplementation, KafkaIOReadProperties>
        notSupportedImplementationsWithProperties = HashMultimap.create();
    for (KafkaIOReadProperties property : KafkaIOReadProperties.values()) {
      final EnumSet<KafkaIOReadImplementation> notSupportedImplementations =
          EnumSet.complementOf(EnumSet.copyOf(property.supportedImplementations));
      if (notSupportedImplementations.isEmpty()) {
        // if we support every implementation we can skip this check
        continue;
      }
      final Object defaultValue = property.getDefaultValue();
      final Object currentValue;
      try {
        currentValue = property.getterMethod.invoke(read);
      } catch (Exception e) {
        throw new RuntimeException("Should not happen", e);
      }
      if (Objects.equals(defaultValue, currentValue)) {
        // the defaultValue is always allowed,
        // so there would be no compatibility issue
        continue;
      }
      // the property has got a value, so we can't allow the not-supported implementations
      for (KafkaIOReadImplementation notSupportedImplementation : notSupportedImplementations) {
        notSupportedImplementationsWithProperties.put(notSupportedImplementation, property);
      }
    }
    if (EnumSet.allOf(KafkaIOReadImplementation.class)
        .equals(notSupportedImplementationsWithProperties.keySet())) {
      throw new IllegalStateException(
          "There is no Kafka read implementation that supports every configured property! "
              + "Not supported implementations with the associated properties: "
              + notSupportedImplementationsWithProperties);
    }
    return new KafkaIOReadImplementationCompatibilityResult(
        notSupportedImplementationsWithProperties);
  }

  static class KafkaIOReadImplementationCompatibilityResult {
    private final Multimap<KafkaIOReadImplementation, KafkaIOReadProperties> notSupported;

    private KafkaIOReadImplementationCompatibilityResult(
        Multimap<KafkaIOReadImplementation, KafkaIOReadProperties>
            notSupportedImplementationsWithAssociatedProperties) {
      this.notSupported = notSupportedImplementationsWithAssociatedProperties;
    }

    boolean supports(KafkaIOReadImplementation implementation) {
      return !notSupported.containsKey(implementation);
    }

    boolean supportsOnly(KafkaIOReadImplementation implementation) {
      return EnumSet.complementOf(EnumSet.of(implementation)).equals(notSupported.keySet());
    }

    void checkSupport(KafkaIOReadImplementation selectedImplementation) {
      checkState(
          supports(selectedImplementation),
          "The current Kafka read configuration isn't supported by the "
              + selectedImplementation
              + " read implementation! "
              + "Conflicting properties: "
              + notSupported.get(selectedImplementation));
    }
  }
}
