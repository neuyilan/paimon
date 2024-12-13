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

import org.apache.paimon.fs.Path;

import org.jetbrains.annotations.TestOnly;

/**
 * Provides paths for table operations based on warehouse, default write locations and read
 * location.
 */
public class PathProvider {
    // the same as the warehouse path;
    private final Path warehouseRootPath;
    // the default write path
    private final Path defaultWriteRootPath;
    // the database name;
    private final String databaseName;
    // the table name;
    private final String tableName;

    @TestOnly
    public PathProvider(Path path) {
        this.warehouseRootPath = path.getParent().getParent();
        this.defaultWriteRootPath = null;
        this.databaseName = path.getParent().getName();
        this.tableName = path.getName();
    }

    public PathProvider(Path path, Path defaultWriteRootPath) {
        this.warehouseRootPath = path.getParent().getParent();
        this.defaultWriteRootPath = defaultWriteRootPath;
        this.databaseName = path.getParent().getName();
        this.tableName = path.getName();
    }

    public PathProvider(
            Path warehouseRootPath,
            Path defaultWriteRootPath,
            String databaseName,
            String tableName) {
        this.warehouseRootPath = warehouseRootPath;
        this.defaultWriteRootPath = defaultWriteRootPath;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public Path tableWritePath() {
        Path location = defaultWriteRootPath != null ? defaultWriteRootPath : warehouseRootPath;
        return new Path(location + "/" + databaseName + "/" + tableName);
    }

    public Path tableReadPath(Path readLocation) {
        Path location = readLocation != null ? readLocation : warehouseRootPath;
        return new Path(location + "/" + databaseName + "/" + tableName);
    }

    public String getWarehouseTablePathString() {
        return getWarehouseTablePath().toString();
    }

    public Path getWarehouseTablePath() {
        return new Path(warehouseRootPath, new Path(databaseName + "/" + tableName));
    }

    public String getWarehouseRootPathString() {
        return warehouseRootPath.toString();
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }
}
