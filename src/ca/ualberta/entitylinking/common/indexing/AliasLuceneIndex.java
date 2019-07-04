/*
 * Copyright 2017 Zhaochen Guo
 *
 * This file is part of WNED.
 * WNED is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * WNED is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with WNED.  If not, see <http://www.gnu.org/licenses/>.
 */
package ca.ualberta.entitylinking.common.indexing;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.StringReader;
import java.io.Reader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FieldType.NumericType;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.IOContext.Context;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.FieldCache.DocTerms;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.spell.NGramDistance;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import ca.ualberta.entitylinking.utils.Rank;
import ca.ualberta.entitylinking.utils.similarity.StringSim;

public class AliasLuceneIndex {
	private static final int NGRAM = 2;
	private AtomicReader reader = null;
	private IndexWriter writer = null;
	private IndexSearcher searcher = null;
	private Analyzer analyzer = null;
	private DocTerms keyArray = null;
	private int[] sizeArray = null;
	private Map<String, Integer> docIDMap = new HashMap<String, Integer>();
	
	private class NGramAnalyzer extends Analyzer {
		private int ngram = 0;
		
		public NGramAnalyzer(int ngram) {
			this.ngram = ngram;
		}
                @Override
                protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
                        Tokenizer source = new NGramTokenizer(reader, 2, ngram);
                        TokenStream filter = new LowerCaseFilter(Version.LUCENE_40, source);
                        return new TokenStreamComponents(source, filter);
                }
	}

	public AliasLuceneIndex() {
		analyzer = new NGramAnalyzer(NGRAM);
	}
	
	public AliasLuceneIndex(String file) {
		analyzer = new NGramAnalyzer(NGRAM);
		index(file);
	}
	
	public void initWriter(String dirLoc) {
		Directory dir = null;
		IndexWriterConfig conf = new IndexWriterConfig(Version.LUCENE_40, analyzer); 
		
		try {
			dir = new MMapDirectory(new File(dirLoc));
			writer = new IndexWriter(dir, conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void addDocument(String alias, List<String> entities) {
		try {
			Document doc = new Document();
			
			Field field = null;

			field = new Field("docID", alias.toLowerCase(), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
			doc.add(field);
			field = new Field("alias", alias.toLowerCase(), Field.Store.YES, 
					Field.Index.ANALYZED, Field.TermVector.YES);
			doc.add(field);
                        TokenStream ts = analyzer.tokenStream("alias", new StringReader(alias.toLowerCase()));
                        int tokenSize = 0;
                        while (ts.incrementToken()) {
                          tokenSize++;
                        }
			IntField nField = new IntField("size", tokenSize, Field.Store.YES);
                        ts.close();
			doc.add(nField);

			for (String entity : entities) {
				field = new Field("content", entity, Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS);
				doc.add(field);
			}
			
			writer.addDocument(doc);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void closeWriter() {
		try {
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("resource")
	public static boolean exists(String diskDir) {
		Directory dir = null;

		try {
			dir = new RAMDirectory(new MMapDirectory(new File(diskDir)), new IOContext(Context.READ));
			if (!DirectoryReader.indexExists(dir))
				return false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	public boolean loadIndex(String diskDir) {
		Directory dir = null;

		try {
			dir = new RAMDirectory(new MMapDirectory(new File(diskDir)), new IOContext(Context.READ));
			if (!DirectoryReader.indexExists(dir))
				return false;
			reader = SlowCompositeReaderWrapper.wrap(IndexReader.open(dir));
                        DocTerms docTerms = FieldCache.DEFAULT.getTerms(reader, "docID");
		//	int[] sizeArray= FieldCache.DEFAULT.getInts(reader, "size");
			searcher = new IndexSearcher(reader);
                        BytesRef term = new BytesRef();
                        for (int i = 0; i < docTerms.size(); i++) {
                                docIDMap.put(docTerms.getTerm(i, term).utf8ToString(), i);
                        }
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("Loading index done22!!");
		return true;
	}
	
	public void index(String file) {
		// aliases to entity Map.
		Map<String, List<String>> a2eMap = new HashMap<String, List<String>>();
		List<String> entList = null;

		try {
			//load a2e map
			String line = null;
			BufferedReader r = new BufferedReader(new FileReader(file));
			while ((line = r.readLine()) != null) {
				String toks[] = line.split("\t");
				String alias = toks[1];
				if (alias == null || alias.isEmpty())
					continue;
				
				alias = alias.toLowerCase().trim();
				if (a2eMap.containsKey(alias))
					entList = a2eMap.get(alias);
				else
					entList = new ArrayList<String>();
				
				entList.add(line);
				a2eMap.put(alias, entList);
			}
			
			r.close();
			
			//build a2e index.
			initWriter("a2eIndex");
			for (String alias : a2eMap.keySet())
				addDocument(alias, a2eMap.get(alias));
			
			closeWriter();
			
			System.out.println("Indexing done33!!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public boolean containsAlias(String aliasName) {
		return docIDMap.containsKey(aliasName.toLowerCase());
	}

	/**
	 * Query the alias index, returns the list of entities using these aliases as 
	 * surface forms.
	 * 
	 * @param queryStr
	 * @return map<entity, alias>
	 */
	public List<String> queryAlias(String queryStr) {
		List<String> ret = new ArrayList<String>();
		
		try {
			queryStr = queryStr.toLowerCase();
			if (!docIDMap.containsKey(queryStr)) {
				System.out.println("No hits");
				return null;
			}
			
			// using an exact match against the keyArray[].
			int docId = docIDMap.get(queryStr);
			Document doc = reader.document(docId);
			IndexableField[] fields = doc.getFields("content");
			for (IndexableField field : fields) 
				ret.add(field.stringValue());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ret;
	}

	public Map<String, List<String>> queryAlias(String queryStr, int n) {
		Map<String, List<String>> ret = new HashMap<String, List<String>>();
		
		try {
                        DocTerms docTerms = null;
			if (docTerms == null)
                                docTerms = FieldCache.DEFAULT.getTerms(reader, "docID");

			queryStr = queryStr.toLowerCase();
			//Just do a quick search
			QueryParser parser = new QueryParser(Version.LUCENE_40, "alias", analyzer);
			Query query = parser.parse(queryStr);
			TopDocs td = searcher.search(query, 200);
			
			if (td == null || td.totalHits == 0) {
				System.out.println("No hits");
				return ret;
			}

			//dice coefficient
			List<Integer> rankList1 = rankingByDiceCoefficient(td, queryStr);
			List<Integer> rankList2 = rankingByNGramDistance(td, queryStr);
			List<Integer> rankList3 = rankingByJaroWinkler(td, queryStr);

			HashSet<Integer> rankList = new HashSet<Integer>();

			int count = (rankList1.size() >= n ? n : rankList1.size());
			for (int i = 0; i < count; i++)
				rankList.add(rankList1.get(i));
			
			count = (rankList2.size() >= n ? n : rankList2.size());
			for (int i = 0; i < count; i++)
				rankList.add(rankList2.get(i));
			
			count = (rankList3.size() >= n ? n : rankList3.size());
			for (int i = 0; i < count; i++)
				rankList.add(rankList3.get(i));
			
			for (Integer docID : rankList) {
				int docId = docID.intValue();
				Document doc = reader.document(docId);
				IndexableField[] fields = doc.getFields("content");

				List<String> list = new ArrayList<String>();
				for (IndexableField field : fields)
					list.add(field.stringValue());
				
                                BytesRef term = new BytesRef();
				ret.put(docTerms.getTerm(docId, term).utf8ToString(), list);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return ret;
	}

	private List<Integer> rankingByDiceCoefficient(TopDocs td, String str) {
                TokenStream ts = null;
                try {
                  ts = analyzer.tokenStream(null, new StringReader(str));
		} catch (Exception e) {e.printStackTrace();}

                CharTermAttribute charTermAttribute = ts.addAttribute(CharTermAttribute.class);
                Map<String, Integer> termFreqVec = new HashMap<String, Integer>();
                try {
                  while (ts.incrementToken()) {
                     String term = charTermAttribute.toString();
                     if(termFreqVec.containsKey(term)){
                        int count = termFreqVec.get(term);
                        termFreqVec.put(term, ++count);
                     }
                     else{
                        termFreqVec.put(term, 1);
                     }
                  }
		} catch (Exception e) {e.printStackTrace();}

		Map<Integer, Double> map = new HashMap<Integer, Double>();
		try {
			if (sizeArray == null)
				sizeArray = FieldCache.DEFAULT.getInts(reader, "size", true);
		} catch (Exception e) {e.printStackTrace();}

                for ( String term: termFreqVec.keySet() ) {
			for (int j = 0; j < td.scoreDocs.length; j++) {
				int docId = td.scoreDocs[j].doc;
				Terms v = null;
				try {
					v = reader.getTermVector(docId, "alias");
				} catch (Exception e) {e.printStackTrace();}

                                TermsEnum reuse = null;
                                TermsEnum termsEnum = null;
				try {
                                  termsEnum = v.iterator(reuse);
                                  if (termsEnum.seekCeil((new Term("alias", term)).bytes()) != SeekStatus.FOUND) {
                                     continue;
                                  }
				} catch (Exception e) {e.printStackTrace();}
				int freq = 0;
				try {
                                   freq = (int)termsEnum.totalTermFreq();
				} catch (Exception e) {e.printStackTrace();}

				double gramScore = (double) 2 * Math.min(termFreqVec.get(term), freq) / (termFreqVec.size() + sizeArray[docId]);
				if (map.containsKey(docId))
					gramScore += map.get(docId);
			
				map.put(docId, gramScore);
			}
		}
	
		//rank the candidates. 
		List<Rank<Double, Integer>> rankList = new ArrayList<Rank<Double, Integer>>();
		for (Integer docId : map.keySet()) {
			double score = map.get(docId);
			Rank<Double, Integer> rank = new Rank<Double, Integer>(score, docId);
			rankList.add(rank);
		}
		
		Collections.sort(rankList);

		List<Integer> ret = new ArrayList<Integer>();
		for (Rank<Double, Integer> rank : rankList) {
			ret.add(rank.obj);
		}
		
		return ret;
	}

	private List<Integer> rankingByNGramDistance(TopDocs td, String str) {
		NGramDistance measure = new NGramDistance(2);
		List<Rank<Double, Integer>> rankList = new ArrayList<Rank<Double, Integer>>();
		
		try {
			if (keyArray == null)
				keyArray = FieldCache.DEFAULT.getTerms(reader, "docID");
		} catch (Exception e) {e.printStackTrace();}

		for (int i = 0; i < td.scoreDocs.length; i++) {
			int docId = td.scoreDocs[i].doc;
			String alias = keyArray.getTerm(docId, null).utf8ToString();
			double sim = measure.getDistance(alias, str);
			rankList.add(new Rank<Double, Integer>(sim, docId));
		}

		Collections.sort(rankList);

		List<Integer> ret = new ArrayList<Integer>();
		for (Rank<Double, Integer> rank : rankList) {
			ret.add(rank.obj);
		}
		
		return ret;
	}

	private List<Integer> rankingByJaroWinkler(TopDocs td, String str) {
		List<Rank<Double, Integer>> rankList = new ArrayList<Rank<Double, Integer>>();

		try {
			if (keyArray == null)
				keyArray = FieldCache.DEFAULT.getTerms(reader, "docID");
		} catch (Exception e) {e.printStackTrace();}

		for (int i = 0; i < td.scoreDocs.length; i++) {
			int docId = td.scoreDocs[i].doc;
			String alias = keyArray.getTerm(docId, null).utf8ToString();
			double sim = StringSim.jaro_winkler_score(alias, str);
			rankList.add(new Rank<Double, Integer>(sim, docId));
		}

		Collections.sort(rankList);

		List<Integer> ret = new ArrayList<Integer>();
		for (Rank<Double, Integer> rank : rankList) {
			ret.add(rank.obj);
		}
		
		return ret;
	}

	public static void main(String[] args) {
		AliasLuceneIndex obj = new AliasLuceneIndex(args[0]);
	}
}
