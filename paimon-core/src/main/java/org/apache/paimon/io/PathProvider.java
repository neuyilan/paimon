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

package org.apache.paimon.io;

import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.fs.Path;

import static org.apache.paimon.catalog.Catalog.DB_SUFFIX;

public class PathProvider {
    // the same as the warehouse path;
    private final String warehouseRootPath;
    // the default write path
    private final String defaultWriteRootPath;
    // the database name;
    private final String databaseName;
    // the table name;
    private final String tableName;

    public PathProvider(
            String warehouseRootPath,
            String defaultWriteRootPath,
            String databaseName,
            String tableName) {
        this.warehouseRootPath = warehouseRootPath;
        this.defaultWriteRootPath = defaultWriteRootPath;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public Path tableWritePath() {
        String location = defaultWriteRootPath != null ? defaultWriteRootPath : warehouseRootPath;
        return new Path(AbstractCatalog.newDatabasePath(location, databaseName), tableName);
    }

    public Path tableReadPath(String readLocation) {
        String location = readLocation != null ? readLocation : warehouseRootPath;
        return new Path(AbstractCatalog.newDatabasePath(location, databaseName), tableName);
    }

    public String getWarehouseTablePathString() {
        return defaultWriteRootPath + "/" + databaseName + DB_SUFFIX + "/" + tableName;
    }

    public Path getWarehouseTablePath() {
        return new Path(
                AbstractCatalog.newDatabasePath(defaultWriteRootPath, databaseName), tableName);
    }

    public String getWarehouseRootPath() {
        return warehouseRootPath;
    }

    public String getDefaultWriteRootPath() {
        return defaultWriteRootPath;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }
}
