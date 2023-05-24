package org.seabow.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.seabow.HdfsDirectory;

public class LuceneIndexer {
    // 索引目录
    private String indexPath = "/user/xuyali/lucene_index";
    // 文本内容
    private String text = "Hello, world! This is a Lucene indexing example3.";
    public static void main(String[] args) {
        new LuceneIndexer().createIndex();
    }
    /**
     * 创建索引
     */
    public void createIndex() {
        try {
            // 索引目录
//            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Directory dir = new HdfsDirectory(new Path(indexPath),new Configuration());
            // 分析器
            StandardAnalyzer analyzer = new StandardAnalyzer();
            // 索引配置
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            // 索引写入器
            IndexWriter writer = new IndexWriter(dir, iwc);
            // 文档
            Document doc = new Document();
            doc.add(new TextField("text", text, Field.Store.YES));
            // 添加文档到索引
            writer.addDocument(doc);
            // 提交事务
            writer.commit();
            // 释放资源
            writer.close();
            dir.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
