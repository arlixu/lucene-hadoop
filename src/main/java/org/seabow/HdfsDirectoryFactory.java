package org.seabow;

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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.spark.SparkEnv;
import org.seabow.cache.store.BlockCache;
import org.seabow.cache.store.BlockDirectory;
import org.seabow.cache.store.BlockDirectoryCache;
import org.seabow.cache.store.BufferStore;
import org.seabow.cache.store.Cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsDirectoryFactory{
    public static Logger LOG = LoggerFactory
            .getLogger(HdfsDirectoryFactory.class);
    public static final String BLOCKCACHE_DIRECT_MEMORY_ALLOCATION = "spark.lucene.blockcache.direct.memory.allocation";
    public static final String BLOCKCACHE_ENABLED = "spark.lucene.blockcache.enabled";
    public static final String BLOCKCACHE_GLOBAL = "spark.lucene.blockcache.global";
    public static final String NRTCACHINGDIRECTORY_ENABLE = "spark.lucene.nrtcachingdirectory.enable";
    public static final String NRTCACHINGDIRECTORY_MAXMERGESIZEMB = "spark.lucene.nrtcachingdirectory.maxmergesizemb";
    public static final String NRTCACHINGDIRECTORY_MAXCACHEMB = "spark.lucene.nrtcachingdirectory.maxcachedmb";
    public static final String NUMBEROFBLOCKSPERBANK = "spark.lucene.blockcache.blocksperbank";

    public static final String KERBEROS_ENABLED = "spark.lucene.security.kerberos.enabled";
    public static final String KERBEROS_KEYTAB = "spark.lucene.security.kerberos.keytabfile";
    public static final String KERBEROS_PRINCIPAL = "spark.lucene.security.kerberos.principal";

    private static BlockCache globalBlockCache;

    public static Metrics metrics;
    private static Boolean kerberosInit;

    private Long maxMemoryForCache(Boolean isDirect){
            //通过hadoop configuration 获取 executor memory和
        SparkEnv sparkEnv = SparkEnv.get();
        Long maxMemoryForCache=-1l;
        if(sparkEnv!=null){
            if(isDirect)
            {
                maxMemoryForCache= SparkEnv.get().memoryManager().maxOnHeapStorageMemory()/2;
            }else {
                maxMemoryForCache= SparkEnv.get().memoryManager().maxOffHeapStorageMemory()/2;
            }
        }
        return maxMemoryForCache;
    }

    public Directory create(String path,Configuration conf)
            throws IOException {
        LOG.info("creating directory factory for path {}", path);
       Boolean blockCacheEnabled= conf.getBoolean(BLOCKCACHE_ENABLED,true);

        if (metrics == null) {
            metrics = new Metrics();
        }

        boolean blockCacheGlobal =  conf.getBoolean(BLOCKCACHE_GLOBAL,true);
        Directory dir = null;

        if (blockCacheEnabled) {
            int numberOfBlocksPerBank =conf.getInt(NUMBEROFBLOCKSPERBANK,16384);
            int blockSize = BlockDirectory.BLOCK_SIZE;
            boolean directAllocation = conf.getBoolean(BLOCKCACHE_DIRECT_MEMORY_ALLOCATION,false);;
            long maxMemoryForCache=maxMemoryForCache(directAllocation);
            LOG.info(
                    "max on-heap memory for cache [{}]", maxMemoryForCache);
            int slabSize = numberOfBlocksPerBank * blockSize;
            int bankCount=(int)(maxMemoryForCache/slabSize);
            if(bankCount>0){
                LOG.info(
                        "Number of slabs of block cache [{}] with direct memory allocation set to [{}]",
                        bankCount, directAllocation);
                LOG.info(
                        "Block cache target memory usage, slab size of [{}] will allocate [{}] slabs and use ~[{}] bytes",
                        new Object[] {slabSize, bankCount,
                                ((long) bankCount * (long) slabSize)});

                int bufferSize = blockSize;
                int bufferCount = 256;

                BlockCache blockCache = getBlockDirectoryCache(numberOfBlocksPerBank,
                        blockSize, bankCount, directAllocation, slabSize,
                        bufferSize, bufferCount, blockCacheGlobal);

                Cache cache = new BlockDirectoryCache(blockCache, path, metrics, blockCacheGlobal);
                HdfsDirectory hdfsDirectory = new HdfsDirectory(new Path(path), conf);
                dir = new BlockDirectory(path, hdfsDirectory, cache, null,
                        true, false);
            }else {
                dir = new HdfsDirectory(new Path(path), conf);
            }
        } else {
            dir = new HdfsDirectory(new Path(path), conf);
        }

        boolean nrtCachingDirectory = conf.getBoolean(NRTCACHINGDIRECTORY_ENABLE,false);
        if (nrtCachingDirectory) {
            double nrtCacheMaxMergeSizeMB = conf.getDouble(NRTCACHINGDIRECTORY_MAXMERGESIZEMB,16);
            double nrtCacheMaxCacheMB =conf.getDouble(NRTCACHINGDIRECTORY_MAXCACHEMB,192);
            return new NRTCachingDirectory(dir, nrtCacheMaxMergeSizeMB,
                    nrtCacheMaxCacheMB);
        }
        return dir;
    }

    private BlockCache getBlockDirectoryCache(int numberOfBlocksPerBank, int blockSize, int bankCount,
                                              boolean directAllocation, int slabSize, int bufferSize, int bufferCount, boolean staticBlockCache) {
        if (!staticBlockCache) {
            LOG.info("Creating new single instance HDFS BlockCache");
            return createBlockCache(numberOfBlocksPerBank, blockSize, bankCount, directAllocation, slabSize, bufferSize, bufferCount);
        }
        synchronized (HdfsDirectoryFactory.class) {

            if (globalBlockCache == null) {
                LOG.info("Creating new global HDFS BlockCache");
                globalBlockCache = createBlockCache(numberOfBlocksPerBank, blockSize, bankCount,
                        directAllocation, slabSize, bufferSize, bufferCount);
            }
        }
        return globalBlockCache;
    }

    private BlockCache createBlockCache(int numberOfBlocksPerBank, int blockSize,
                                        int bankCount, boolean directAllocation, int slabSize, int bufferSize,
                                        int bufferCount) {
        BufferStore.initNewBuffer(bufferSize, bufferCount);
        long totalMemory = (long) bankCount * (long) numberOfBlocksPerBank
                * (long) blockSize;

        BlockCache blockCache;
        try {
            blockCache = new BlockCache(metrics, directAllocation, totalMemory, slabSize, blockSize);
        } catch (OutOfMemoryError e) {
            throw new RuntimeException(
                    "The max direct memory is likely too low.  Either increase it (by adding -XX:MaxDirectMemorySize=<size>g -XX:+UseLargePages to your containers startup args)"
                            + " or disable direct allocation using spark.lucene.blockcache.direct.memory.allocation=false. If you are putting the block cache on the heap,"
                            + " your java heap size might not be large enough."
                            + " Failed allocating ~" + totalMemory / 1000000.0 + " MB.",
                    e);
        }
        return blockCache;
    }


    }