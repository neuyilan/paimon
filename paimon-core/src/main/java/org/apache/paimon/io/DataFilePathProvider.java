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

import org.apache.paimon.data.BinaryRow;

import java.io.Serializable;

/** Provides the path to the data file. */
public class DataFilePathProvider implements Serializable {
    private TablePathProvider tablePathProvider;
    private BinaryRow partition;
    private int bucket;

    public DataFilePathProvider(
            TablePathProvider tablePathProvider, BinaryRow partition, int bucket) {
        this.tablePathProvider = tablePathProvider;
        this.partition = partition;
        this.bucket = bucket;
    }

    public TablePathProvider getTablePathProvider() {
        return tablePathProvider;
    }

    public void setTablePathProvider(TablePathProvider tablePathProvider) {
        this.tablePathProvider = tablePathProvider;
    }

    public BinaryRow getPartition() {
        return partition;
    }

    public void setPartition(BinaryRow partition) {
        this.partition = partition;
    }

    public int getBucket() {
        return bucket;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }
}
