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
package org.seabow;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;



public class Metrics {


    public AtomicLong blockCacheSize = new AtomicLong(0);
    public AtomicLong blockCacheHit = new AtomicLong(0);
    public AtomicLong blockCacheMiss = new AtomicLong(0);
    public AtomicLong blockCacheEviction = new AtomicLong(0);
    public AtomicLong blockCacheStoreFail = new AtomicLong(0);

    // since the last call
    private AtomicLong blockCacheHit_last = new AtomicLong(0);
    private AtomicLong blockCacheMiss_last = new AtomicLong(0);
    private AtomicLong blockCacheEviction_last = new AtomicLong(0);
    public AtomicLong blockCacheStoreFail_last = new AtomicLong(0);


    // These are used by the BufferStore (just a generic cache of byte[]).
    // TODO: If this (the Store) is a good idea, we should make it more general and use it across more places in Solr.
    public AtomicLong shardBuffercacheAllocate = new AtomicLong(0);
    public AtomicLong shardBuffercacheLost = new AtomicLong(0);

}