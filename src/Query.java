/*
 * Mr. Komson Najard 5988020 Sec 1
 * Mr. Thanadon Bunkurd 5988073 Sec 1
 * Mr. Chanathip Pornprasit 5988179 Sec 1
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class Query {
	public static String[] queriesSmall = {
			"hello",
			"bye",
			"you",
			"how are you",
			"how are you ?"
	};
	// Term id -> position in index file
	private  Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private  Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private  Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private  Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private  BaseIndex index = null;
	

	//indicate whether the query service is running or not
	private boolean running = false;
	private RandomAccessFile indexFile = null;
	
	/* 
	 * Read a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private  PostingList readPosting(FileChannel fc, int termId)
			throws IOException {
		/*
		 * TODO: Your code here
		 */
		
		return index.readPosting(fc.position(posDict.get(termId)));
	}
	
	
	public void runQueryService(String indexMode, String indexDirname) throws IOException
	{
		//Get the index reader
		try {
			Class<?> indexClass = Class.forName(indexMode+"Index");
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		//Get Index file
		File inputdir = new File(indexDirname);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + indexDirname);
			return;
		}
		
		/* Index file */
		indexFile = new RandomAccessFile(new File(indexDirname,
				"corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(
				indexDirname, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(
				indexDirname, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(
				indexDirname, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]),
					Integer.parseInt(tokens[2]));
		}
		postReader.close();
		
		this.running = true;
	}
    
	public List<Integer> retrieve(String query) throws IOException
	{	if(!running) 
		{
			System.err.println("Error: Query service must be initiated");
		}
		
		/*
		 * TODO: Your code here
		 *       Perform query processing with the inverted index.
		 *       return the list of IDs of the documents that match the query
		 *      
		 */
		int tid;
		List<List<Integer>> alllist = new ArrayList<>();
		String split[] = query.split(" ");
		for(String str: split)
		{
			// get posting list for each str
			// then add all doc ids of each posting list to docids
			
			if(termDict.get(str) != null)
			{
				//List<Integer> docids = new ArrayList<>();
				FileChannel fc = indexFile.getChannel();
				tid = termDict.get(str);
				//System.out.println(str + " " + tid);
				PostingList pl = readPosting(fc, tid);
//				for(Integer a: pl.getList())
//					docids.add(a);
				alllist.add(pl.getList());
			}
		}
		
		//merge set T-T
		while(alllist.size() > 1)
		{
			List<Integer> posting1 = alllist.remove(0);
			List<Integer> posting2 = alllist.remove(0);
			List<Integer> merged = new ArrayList<>();
			
			int s1 = posting1.size();
			int s2 = posting2.size();
			int i1 = 0;
			int i2 = 0;
			
			while(s1 > 0 && s2 > 0)
			{
				if(posting1.get(i1) == posting2.get(i2))
				{
					merged.add(posting1.get(i1));
					i1++; s1--;
					i2++; s2--;
				}
				else if(posting1.get(i1) < posting2.get(i2))
				{
					i1++;
					s1--;
				}
					
				else
				{
					i2++;
					s2--;
				}

			}
			alllist.add(merged);
		}
		//System.out.println(docids);
		if(alllist.size() > 0) return alllist.remove(0);
		else return new ArrayList<Integer>();
		
	}
	
    String outputQueryResult(List<Integer> res) {
        /*
         * TODO: 
         * 
         * Take the list of documents ID and prepare the search results, sorted by lexicon order. 
         * 
         * E.g.
         * 	0/fine.txt
		 *	0/hello.txt
		 *	1/bye.txt
		 *	2/fine.txt
		 *	2/hello.txt
		 *
		 * If there no matched document, output:
		 * 
		 * no results found
		 * 
         * */
    	
    	if(res.size() == 0) return "no results found";
    	
    	StringBuilder sb = new StringBuilder();
    	Set<String> result = new TreeSet<String>();
    	for(Integer a: res)
    	{
    		result.add(docDict.get(a));
    	}
    	
    	for(String str: result)
    		sb.append(str+"\n");
    	return sb.toString();
    }
	
	public static void main(String[] args) throws IOException {
		/* Parse command line */
//		if (args.length != 2) {
//			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
//			return;
//		}
//
//		/* Get index */
//		String className = null;
//		try {
//			className = args[0];
//		} catch (Exception e) {
//			System.err
//					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
//			throw new RuntimeException(e);
//		}
//
//		/* Get index directory */
//		String input = args[1];
//		
//		Query queryService = new Query();
//		queryService.runQueryService(className, input);
//		
//		/* Processing queries */
//		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//
//		/* For each query */
//		String line = null;
//		while ((line = br.readLine()) != null) {
//			List<Integer> hitDocs = queryService.retrieve(line);
//			queryService.outputQueryResult(hitDocs);
//		}
//		br.close();
		P1Tester.testQuery("Basic", "./index/small", queriesSmall, "./output/small");
	}
	
	protected void finalize()
	{
		try {
			if(indexFile != null)indexFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

