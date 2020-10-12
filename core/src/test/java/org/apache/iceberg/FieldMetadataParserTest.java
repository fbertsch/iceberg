/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class FieldMetadataParserTest {

  private final static String longJson = "{\"longKey\":3}";

  private final static String doubleJson = "{\"doubleKey\":3.5}";

  private final static String booleanJson = "{\"booleanKey\":false}";

  private final static String stringJson = "{\"stringKey\":\"stringValue\"}";

  private final static String longArrayJson = "{\"longArrayKey\":[3,5]}";

  private final static String doubleArrayJson = "{\"doubleArrayKey\":[3.1,5.2]}";

  private final static String booleanArrayJson = "{\"booleanArrayKey\":[true,false,true]}";

  private final static String stringArrayJson = "{\"stringArrayKey\":[\"val1\",\"val2\"]}";

  private final static String nestedJson =
      "{\"l0Key\":{\"stringKey\":\"stringValue\",\"l1Key\":{\"doubleKey\":2.5}}}";

  private final static String nestedArrayJson =
      "{\"l0Key\":[{\"stringKey\":\"stringValue\"},{\"doubleKey\":2.5},{\"longKey\":33}]}";

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {
            "Simple",
            ImmutableMap.of(
                1, longJson,
                2, doubleJson,
                3, booleanJson,
                4, stringJson),
            new StringJoiner(",", "{", "}")
                .add(String.join(":", "\"1\"", longJson))
                .add(String.join(":", "\"2\"", doubleJson))
                .add(String.join(":", "\"3\"", booleanJson))
                .add(String.join(":", "\"4\"", stringJson))
                .toString()
        },
        {
            "Array",
            ImmutableMap.of(
                11, longArrayJson,
                12, doubleArrayJson,
                13, booleanArrayJson,
                14, stringArrayJson),
            new StringJoiner(",", "{", "}")
                .add(String.join(":", "\"11\"", longArrayJson))
                .add(String.join(":", "\"12\"", doubleArrayJson))
                .add(String.join(":", "\"13\"", booleanArrayJson))
                .add(String.join(":", "\"14\"", stringArrayJson))
                .toString()
        },
        {
            "Nested",
            ImmutableMap.of(9, nestedJson),
            new StringJoiner(",", "{", "}")
                .add(String.join(":", "\"9\"", nestedJson))
                .toString()
        },
        {
            "NestedArray",
            ImmutableMap.of(19, nestedArrayJson),
            new StringJoiner(",", "{", "}")
                .add(String.join(":", "\"19\"", nestedArrayJson))
                .toString()
        }
    });
  }

  @Parameterized.Parameter
  public String testName;

  @Parameterized.Parameter(1)
  public Map<Integer, String> metadata;

  @Parameterized.Parameter(2)
  public String metadataJson;

  @Test
  public void toJson() {
    assertEquals(metadataJson, FieldMetadataParser.toJson(metadata));
  }

  @Test
  public void fromJson() {
    assertEqualsMetadata(metadata, Objects.requireNonNull(FieldMetadataParser.fromJson(metadataJson)));
  }

  private void assertEqualsMetadata(Map<Integer, String> first, Map<Integer, String> second) {
    assertEquals(first.size(), second.size());
    first.forEach((key, value) -> assertEquals(value, second.get(key)));
  }
}
