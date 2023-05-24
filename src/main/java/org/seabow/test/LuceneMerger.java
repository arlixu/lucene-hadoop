package org.seabow.test;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.seabow.HdfsDirectory;

public class LuceneMerger {
    // 索引目录
    private String indexPath = "/user/xuyali/lucene_index";
    public static void main(String[] args) {
        new LuceneMerger().mergeIndex();
    }
    /**
     * 合并索引
     */
    public void mergeIndex() {
        try {
            // 索引目录
            Directory dir = new HdfsDirectory(new Path(indexPath),new Configuration());
            // 索引写入器配置
            IndexWriterConfig config = new IndexWriterConfig();
            // 索引写入器
            IndexWriter writer = new IndexWriter(dir, config);
            // 强制合并所有片段
            writer.forceMerge(1);
            // 释放资源
            writer.commit();
            writer.close();
            dir.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}