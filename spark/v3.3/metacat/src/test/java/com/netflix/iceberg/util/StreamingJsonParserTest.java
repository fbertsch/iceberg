package com.netflix.iceberg.util;

import com.google.common.collect.Maps;
import org.apache.iceberg.Files;
import org.apache.iceberg.io.InputFile;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class StreamingJsonParserTest {


    public static final String TABLE_UUID = "table-uuid";
    public static final String LOCATION = "location";
    public static final String PROPERTIES = "properties";

    @Test
    public void testParseJsonFile() throws IOException {
        String location = "src/test/resources/test_metadata.json";
        InputFile file = Files.localInput(location);
        StreamingJsonParser jasonParser = new StreamingJsonParser(file.newStream());

        Map<String, Class<?>> fields = Maps.newHashMap();
        fields.put(TABLE_UUID, String.class);
        fields.put(LOCATION, String.class);
        fields.put(PROPERTIES, Map.class);

        Map<String, Object> result = jasonParser.parse(fields);

        Assertions.assertThat(result)
                .hasSize(fields.size())
                .hasEntrySatisfying(TABLE_UUID, value -> Assertions.assertThat(value).isEqualTo("fffffff-ffff-ffff-ffff-fffffffffffff"))
                .hasEntrySatisfying(LOCATION, value -> Assertions.assertThat(value).isEqualTo("s3://secure-bucket/iceberg/warehouse/default.db/tbl"))
                .hasEntrySatisfying(PROPERTIES, value -> {
                    Assertions.assertThat(value).isInstanceOf(Map.class);
                    Assertions.assertThat((Map<String, String>) value)
                            .hasSize(3)
                            .hasEntrySatisfying("owner", v -> Assertions.assertThat(v).isEqualTo("owner"))
                            .hasEntrySatisfying("acls", v -> Assertions.assertThat(v).isEqualTo("[{\"format_version\":1,\"principals\":[{\"name\":\"123456789\",\"principal_type\":\"USER\"}],\"resources\":[{\"resource_type\":\"TABLE\",\"uuid\":\"ffffff-ffff-ffff-ffff\",\"parent\":{\"resource_type\":\"SCHEMA\",\"name\":\"user_name\",\"parent\":{\"resource_type\":\"CATALOG\",\"name\":\"catalog\",\"parent\":null}}}],\"privileges\":[\"SELECT\",\"INSERT\",\"DELETE\",\"UPDATE\"],\"grantee\":{\"name\":\"123456789\",\"principal_type\":\"USER\"},\"with_grant\":true}]"))
                            .hasEntrySatisfying("write.format.default", v -> Assertions.assertThat(v).isEqualTo("PARQUET"));
                });
    }
}
