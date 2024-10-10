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
package org.apache.iceberg.spark.extensions;

import java.util.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestGetTablesProcedure extends SparkExtensionsTestBase {

    public TestGetTablesProcedure(
            String catalogName, String implementation, Map<String, String> config) {
        super(catalogName, implementation, config);
    }

    @After
    public void removeTables() {
        sql("DROP TABLE IF EXISTS %s", tableName);
    }

    @Test
    public void testSingleTableQuery() {
        sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
        sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

        String query = String.format("SELECT * FROM %s", tableName);

        List<Object[]> output =
                sql(
                        "CALL %s.system.get_tables("
                                + "query_or_view => '%s')",
                        catalogName, query
                );

        Assert.assertEquals("Should return exactly one table", 1, output.size());

        String resultTableName = (String) output.get(0)[0];
        Boolean isTable = (Boolean) output.get(0)[1];

        String expectedTableName = tableName.toLowerCase();
        if (!expectedTableName.toLowerCase().startsWith(catalogName.toLowerCase())) {
            expectedTableName = String.format("%s.%s", catalogName, tableName).toLowerCase();
        }
        Assert.assertEquals("Should return the correct table name", expectedTableName, resultTableName.toLowerCase());
        Assert.assertTrue("Result should be a table", isTable);
    }

    @Test
    public void testComplexSelectQuery() {
        sql("CREATE TABLE %s.default.customers (id INT, name STRING, email STRING) USING iceberg", catalogName);
        sql("CREATE TABLE %s.default.orders (id INT, customer_id INT, order_date DATE, total DECIMAL(10,2)) USING iceberg", catalogName);
        sql("CREATE TABLE %s.default.products (id INT, name STRING, price DECIMAL(10,2)) USING iceberg", catalogName);
        sql("CREATE TABLE %s.default.order_items (order_id INT, product_id INT, quantity INT) USING iceberg", catalogName);

        try {
            String query = String.format(
                    "WITH recent_orders AS (" +
                            "    SELECT customer_id, COUNT(*) as order_count " +
                            "    FROM %1$s.default.orders " +
                            "    WHERE order_date >= DATE_SUB(CURRENT_DATE(), 30) " +
                            "    GROUP BY customer_id" +
                            "), " +
                            "high_value_customers AS (" +
                            "    SELECT c.id, c.name, ro.order_count " +
                            "    FROM %1$s.default.customers c " +
                            "    JOIN recent_orders ro ON c.id = ro.customer_id " +
                            "    WHERE ro.order_count >= 3" +
                            ") " +
                            "SELECT hvc.name AS customer_name, " +
                            "       p.name AS product_name, " +
                            "       SUM(oi.quantity) AS total_quantity " +
                            "FROM high_value_customers hvc " +
                            "JOIN %1$s.default.orders o ON hvc.id = o.customer_id " +
                            "JOIN %1$s.default.order_items oi ON o.id = oi.order_id " +
                            "JOIN %1$s.default.products p ON oi.product_id = p.id " +
                            "GROUP BY hvc.name, p.name " +
                            "ORDER BY total_quantity DESC " +
                            "LIMIT 10",
                    catalogName
            );

            List<Object[]> output =
                    sql(
                            "CALL %s.system.get_tables("
                                    + "query_or_view => '%s')",
                            catalogName, query
                    );

            Set<String> expectedTables = new HashSet<>(Arrays.asList(
                    catalogName + ".default.customers",
                    catalogName + ".default.orders",
                    catalogName + ".default.products",
                    catalogName + ".default.order_items"
            ));

            Set<String> actualTables = new HashSet<>();
            for (Object[] row : output) {
                String tableName = (String) row[0];
                Boolean isTable = (Boolean) row[1];
                Assert.assertTrue("Each result should be a table", isTable);
                actualTables.add(tableName);
            }

            Assert.assertEquals("All tables should be detected", expectedTables, actualTables);
        } finally {
            sql("DROP TABLE IF EXISTS %s.default.customers", catalogName);
            sql("DROP TABLE IF EXISTS %s.default.orders", catalogName);
            sql("DROP TABLE IF EXISTS %s.default.products", catalogName);
            sql("DROP TABLE IF EXISTS %s.default.order_items", catalogName);
        }
    }
}