/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs.lucene90;

import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * doc values block encoder
 */
public interface BaseEncoder {
    /**
     * add a value
     * @param index the value's index
     * @param value the value
     */
    void add(int index, long value);

    /**
     * encode buffer data to output
     * @param out data output
     * @throws IOException if io exception
     */
    void encode(DataOutput out) throws IOException;

    /**
     * get a value
     * @param index the value's index
     * @return the value of index
     */
    long get(int index);

    /**
     * decode input data to buffer
     * @param in data input
     * @throws IOException if io exception
     */
    void decode(DataInput in) throws IOException;
}
