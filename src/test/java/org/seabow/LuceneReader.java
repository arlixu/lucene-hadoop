package org.seabow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;

public class LuceneReader {
    // 索引目录
    /**
     * 搜索索引
     */
    public void searchIndex(String indexPath) {
        try {
            // 索引目录
            Directory dir = new HdfsDirectory(new Path(indexPath),new Configuration());
            // 索引读取器
            IndexReader reader = DirectoryReader.open(dir);
            // 索引搜索器
            IndexSearcher searcher = new IndexSearcher(reader);
            // 分析器
            StandardAnalyzer analyzer = new StandardAnalyzer();
            // 查询解析器
            // 解析查询字符串
//            Query query = parser.parse(queryStr);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
            Query query=builder.build();
            // 搜索
            TopDocs results = searcher.search(query, 10);
            ScoreDoc[] hits = results.scoreDocs;
            // 遍历结果
            for (ScoreDoc hit : hits) {
                int docId = hit.doc;
                Document doc = searcher.doc(docId);
                System.out.println(docId);
                System.out.println(doc);
            }
            // 释放资源
            reader.close();
            dir.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}