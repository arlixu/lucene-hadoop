package org.seabow.test;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.seabow.HdfsDirectory;

public class LuceneUpdater {
    // 索引目录
    private String indexPath = "/user/xuyali/lucene_index";
    public static void main(String[] args) {
        new LuceneUpdater().updateIndex();
    }
    /**
     * 更新索引
     */
    public void updateIndex() {
        try {
            // 索引目录
            Directory dir = new HdfsDirectory(new Path(indexPath),new Configuration());
            // 索引写入器配置
            IndexWriterConfig config = new IndexWriterConfig();
            // 索引写入器
            IndexWriter writer = new IndexWriter(dir, config);
            // 创建文档
            Document doc = new Document();
            doc.add(new TextField("id", "1", Field.Store.YES));
            doc.add(new TextField("title", "Lucene in Action", Field.Store.YES));
            doc.add(new TextField("content", "Lucene is a search engine library.", Field.Store.YES));
            // 更新文档
            writer.updateDocument(new Term("text", "indexing"), doc);
            // 提交更改
            writer.commit();
            // 关闭资源
            writer.close();
            dir.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}