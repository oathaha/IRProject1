

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
import java.util.TreeMap;

public class Query {

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
		PostingList post = new PostingList(termId);
		Long pos = posDict.get();
		return post;
		
		/*
		 * 1. create docList (List<Integer> blabla = new ...)
		 * 2. get term position from posDict (this integer is byte position in index file)
		 * 3. get frequency of document from freqDict
		 * 4. call IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, fc.size(), <term position in 1.>)
		 * 	  .asIntBuffer();
		 * 5. pos <- term position in 1.
		 * 6. pos = pos + 2 (because pos firstly point to term id and the next number is length of posting list,
		 * 	  we want to skip both numbers)
		 * 7. while frequency of document > 0
		 * 		get docID from ib (ib.get(pos/4)) // divide by 4 because each index represents 4 bytes
		 * 		add docID to docList
		 * 		pos+=4 // move to the next integer (or next index)
		 * 		decrease frequency of document by 1
		 * 8. create posting list (PostingList pl = new ...)
		 * 9. return posting list
		 */
		
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
		else {
			List<Integer> list = new ArrayList<Integer>();
			for(Map.Entry<String, Integer> entry: termDict.entrySet()) {
				
			}
		}
		
		/*
		 * 1. get term id from input query
		 * 2. create FileChannel for indexFile (look at line 34 for more ideas na)
		 * 3. call readPosting() by sending parameters in 1. and 2. to get posting list
		 * 4. take document id list from the posting list in 3. then return it
		 */
		return null;
		
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
    	
    	/*
		 * if size of res is 0 then return "no results found"
		 * else
		 * 	create empty string builder (StringBuilder class)
		 * 	for each docID in res
		 * 		get doc name from docDict
		 * 		append doc name to string builder
		 * 	return string builder as string (StringBuilder.toString)
		 */
    	return null;
    }
	
	public static void main(String[] args) throws IOException {
		String[] queriesSmall = {
				"hello",
				"bye",
				"you",
				"how are you",
				"how are you ?"
		};
		/* Parse command line */
		/*if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
			return;
		}

		 Get index 
		String className = null;
		try {
			className = args[0];
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}

		 Get index directory 
		String input = args[1];
		
		Query queryService = new Query();
		queryService.runQueryService(className, input);
		
		 Processing queries 
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		 For each query 
		String line = null;
		while ((line = br.readLine()) != null) {
			List<Integer> hitDocs = queryService.retrieve(line);
			queryService.outputQueryResult(hitDocs);
		}
		
		br.close();*/
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

