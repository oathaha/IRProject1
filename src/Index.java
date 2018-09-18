
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
public class Index { 

	// Term id -> (position in index file, doc frequency) dictionary
	private static Map<Integer, Pair<Long, Integer>> postingDict 
		= new TreeMap<Integer, Pair<Long, Integer>>();
	
	// Doc name -> doc id dictionary
	// docDict.put("doc_name",doc_id)
	private static Map<String, Integer> docDict
		= new TreeMap<String, Integer>();
	
	// Term -> term id dictionary
	// termDict.put("term",term_id)
	private static Map<String, Integer> termDict
		= new TreeMap<String, Integer>();
	// Block queue
	private static LinkedList<File> blockQueue
		= new LinkedList<File>();

	private static TreeMap<Integer, TreeSet<Integer>> TeeMap = new TreeMap<Integer, TreeSet<Integer>>();
	
	// Total file counter   
	private static int totalFileCount = 0;
	// Document counter
	private static int docIdCounter = 0;
	// Term counter
	private static int wordIdCounter = 0;
	// Index
	private static BaseIndex index = null;

	
	/* 
	 * Write a posting list to the given file 
	 * You should record the file position of this posting list
	 * so that you can read it back during retrieval
	 * 
	 * */
	private static void writePosting(FileChannel fc, PostingList posting)
			throws IOException {
		/*
		 * TODO: Your code here
		 *	 
		 */
		IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, fc.size(), (posting.getList().size()+2)*4).asIntBuffer();
		ib.put(posting.getTermId());
		ib.put(posting.getList().size());
		for(Integer a: posting.getList())
			ib.put(a);
	}
	
	public void hello()
	{
		
	}
	 /**
     * Pop next element if there is one, otherwise return null
     * @param iter an iterator that contains integers
     * @return next element or null
     */
    private static Integer popNextOrNull(Iterator<Integer> iter) {
        if (iter.hasNext()) {
            return iter.next();
        } else {
            return null;
        }
    }
	
    private static void mergeblock(RandomAccessFile bf1, RandomAccessFile bf2, RandomAccessFile mf) throws IOException
    {
    	FileChannel fc1 = bf1.getChannel();
    	FileChannel fc2 = bf2.getChannel();
    	FileChannel fc3 = mf.getChannel();
		IntBuffer ib1 = fc1.map(FileChannel.MapMode.READ_ONLY, 0, fc1.size()).asIntBuffer();
		IntBuffer ib2 = fc2.map(FileChannel.MapMode.READ_ONLY, 0, fc2.size()).asIntBuffer();
	
		Set<Integer> combinedocid = new TreeSet<Integer>();
		List<Integer> allcombine = new ArrayList<Integer>();
		int i1 = 0; // position for bf1
		int i2 = 0; // position for bf2
		int sizef1 = (int)fc1.size()/4; // total number of int in fc1
		int sizef2 = (int)fc2.size()/4; // total number of int in fc2
		int l; // for loop
		//System.out.println(sizef1 + " " + sizef2);
		try
		{
			while(true)
			{
				int tid1 = ib1.get(i1);	i1++;
				int tid2 = ib2.get(i2);	i2++;
				int doclen1 = ib1.get(i1); i1++;
				int doclen2 = ib2.get(i2); i2++;
				int c1 = 0; // count doc in bf1
				int c2 = 0; // count doc in bf2
				
				sizef1-=2;
				sizef2-=2;
				
				if(tid1 == tid2) // we can merge posting list
				{
					// end of list when c1 == doclen1 or c2 == doclen2
					while((c1 < doclen1) && (c2 < doclen2)) // merge 2 posting list
					{
						if(ib1.get(i1) == ib2.get(i2))
						{
							combinedocid.add(ib1.get(i1));
							i1++; c1++; sizef1--;
							i2++; c2++; sizef2--;
						}
						else if(ib1.get(i1) < ib1.get(i2))
						{
							combinedocid.add(ib1.get(i1));
							i1++; c1++; sizef1--;
						}
						else
						{
							combinedocid.add(ib2.get(i2));
							i2++; c2++; sizef2--;
						}
					}
					
					for(l = c1; l<doclen1; l++)
					{
						combinedocid.add(ib1.get(i1));
						sizef1--;
						i1++;
					}
					
					for(l = c2; l<doclen2; l++)
					{
						combinedocid.add(ib2.get(i2));
						sizef2--;
						i2++;
					}

					//ib3.put(tid1);
					//ib3.put(combinedocid.size());
					allcombine.add(tid1);
					allcombine.add(combinedocid.size());
					for(Integer a: combinedocid)
						//ib3.put(a);
						allcombine.add(a);
					combinedocid.clear();
					
				}
				else if(tid1 < tid2)
				{
//					ib3.put(tid1);
//					ib3.put(doclen1);
					allcombine.add(tid1);
					allcombine.add(doclen1);
					for(l = i1; l<i1+doclen1; l++)
					{
						//ib3.put(ib1.get(i1));
						allcombine.add(ib1.get(i1));
						sizef1--;
						i1++;
					}
						
					//i1 = l;
					//i1+=doclen1;
					//sizef1 -= doclen1;
				}
				else
				{
//					ib3.put(tid2);
//					ib3.put(doclen2);
					allcombine.add(tid2);
					allcombine.add(doclen2);
					for(l = i2; l<i2+doclen2; l++)
					{
						//ib3.put(ib2.get(i2));
						allcombine.add(ib2.get(i2));
						sizef2--;
						i2++;
					}
						
					//i2 = l;
					//i2+=doclen2;
					//sizef2 -= doclen2;
				}
			}
		}
		catch(Exception e)
		{
			// put the rest of file to mf
			// if sizef1 = i1 then put the rest from bf2 to mf
			// if sizef2 = i2 then put the rest from bf1 to mf
			//System.out.println("size f1 = " + sizef1);
			//System.out.println("size f2 = " + sizef2);
			if(sizef1 == 0) // reach the end of bf1
			{
				for(int a = i2; a < fc2.size()/4; a++)
				{
					//ib3.put(ib2.get(a));
					allcombine.add(ib2.get(a));
				}
				//System.out.println("put f2 all");
			}
			else if(sizef2 == 0) // reach the end of bf2
			{
				for(int a = i1; a < fc1.size()/4; a++)
				{
					//ib3.put(ib1.get(a));
					allcombine.add(ib1.get(a));
				}
				//System.out.println("put f1 all");
			}
			IntBuffer ib3 = fc3.map(FileChannel.MapMode.READ_WRITE, 0, allcombine.size()*4).asIntBuffer();
			for(Integer a: allcombine)
				ib3.put(a);
			
			bf1.close();
			bf2.close();
			mf.close();
			fc1.close(); 
			fc2.close(); 
			fc3.close();
			//System.out.println("f1 size: " + sizef1 + ", f2 size: " + sizef2);
			//System.out.println("Merge done");
		}
    }
   
	
	/**
	 * Main method to start the indexing process.
	 * @param method		:Indexing method. "Basic" by default, but extra credit will be given for those
	 * 			who can implement variable byte (VB) or Gamma index compression algorithm
	 * @param dataDirname	:relative path to the dataset root directory. E.g. "./datasets/small"
	 * @param outputDirname	:relative path to the output directory to store index. You must not assume
	 * 			that this directory exist. If it does, you must clear out the content before indexing.
	 */
	public static int runIndexer(String method, String dataDirname, String outputDirname) throws IOException 
	{
		/* Get index */
		String className = method + "Index";
		try {
			Class<?> indexClass = Class.forName(className);
			index = (BaseIndex) indexClass.newInstance();
		} catch (Exception e) {
			System.err
					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
			throw new RuntimeException(e);
		}
		
		/* Get root directory */
		File rootdir = new File(dataDirname);
		if (!rootdir.exists() || !rootdir.isDirectory()) {
			System.err.println("Invalid data directory: " + dataDirname);
			return -1;
		}
		
		   
		/* Get output directory*/
		File outdir = new File(outputDirname);
		if (outdir.exists() && !outdir.isDirectory()) {
			System.err.println("Invalid output directory: " + outputDirname);
			return -1;
		}
		
		/*	TODO: delete all the files/sub folder under outdir
		 * 
		 */
		
		
		/*calldelete line 264-294*/
		deleteDir(outdir);
		
		
		if (!outdir.exists()) {
			if (!outdir.mkdirs()) {
				System.err.println("Create output directory failure");
				return -1;
			}
		}
		
		
		/*Combine to make Map*/
		
		/* BSBI indexing algorithm */
		File[] dirlist = rootdir.listFiles();

		/* For each block */
		for (File block : dirlist) {
			File blockFile = new File(outputDirname, block.getName());
			System.out.println("Processing block "+block.getName());
			blockQueue.add(blockFile);

			File blockDir = new File(dataDirname, block.getName());
			File[] filelist = blockDir.listFiles();
			
			/* For each file */
			for (File file : filelist) {
				++totalFileCount;
				String fileName = block.getName() + "/" + file.getName();
				
				 // use pre-increment to ensure docID > 0
                int docId = ++docIdCounter;
                docDict.put(fileName, docId);
				
                

				BufferedReader reader = new BufferedReader(new FileReader(file));
				String line;
				while ((line = reader.readLine()) != null) {
					String[] tokens = line.trim().split("\\s+");
					for (String token : tokens) {
						/*
						 * TODO: Your code here
						 *       For each term, build up a list of
						 *       documents in which the term occurs
						 */
						// for term dict
							if(!termDict.containsKey(token)) {
								termDict.put(token, ++wordIdCounter);
							}
							
						// for posting list
							int termId = termDict.get(token);
							if(!TeeMap.containsKey(termId)) {
								TeeMap.put(termId, new TreeSet<Integer>());
							}
							TeeMap.get(termId).add(docId);
					}
				}
				reader.close();
			}

			/* Sort and output */
			if (!blockFile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}
			
			RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");

			/*
			 * TODO: Your code here
			 *       Write all posting lists for all terms to file (bfc) 
			 */
			FileChannel fc = bfc.getChannel();
			for(Integer termId : TeeMap.keySet() ) 
			{
				writePosting(fc, new PostingList(termId, new ArrayList<Integer>(TeeMap.get(termId))));
//				System.out.println("term id = " + termId + " doclen = " + TeeMap.get(termId).size());
//				System.out.print("posting list: "+ TeeMap.get(termId));
//				System.out.println();
			}
			fc.close();
			bfc.close();
		}

		/* Required: output total number of files. */
		//System.out.println("Total Files Indexed: "+totalFileCount);

		/* Merge blocks */
		while (true) {
			if (blockQueue.size() <= 1)
				break;

			File b1 = blockQueue.removeFirst();
			File b2 = blockQueue.removeFirst();
			
			File combfile = new File(outputDirname, b1.getName() + "+" + b2.getName());
			if (!combfile.createNewFile()) {
				System.err.println("Create new block failure.");
				return -1;
			}

			RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
			RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
			RandomAccessFile mf = new RandomAccessFile(combfile, "rw");
			 
			/*
			 * TODO: Your code here
			 *       Combine blocks bf1 and bf2 into our combined file, mf
			 *       You will want to consider in what order to merge
			 *       the two blocks (based on term ID, perhaps?).
			 *       
			 */
			
			/*
			 * 1. read data from bf1
			 * 2. read data from bf2
			 * 3. do merging algorithm ??? (merge to mf file)
			 */
			
			mergeblock(bf1,bf2,mf);
			bf1.close();
			bf2.close();
			mf.close();
			b1.delete();
			b2.delete();
			blockQueue.add(combfile);
		}

		
		/* Dump constructed index back into file system */
//		System.out.println("block queue size: " + blockQueue.size());
//		System.out.println(blockQueue.removeFirst().getName());
		File indexFile = blockQueue.removeFirst();
		indexFile.renameTo(new File(outputDirname, "corpus.index")); // fail, why???
		
		
		// create postingDict here???
		RandomAccessFile index = new RandomAccessFile(indexFile, "rw");
		FileChannel fc = index.getChannel();
		IntBuffer ibf = fc.map(FileChannel.MapMode.READ_WRITE, 0, fc.size()).asIntBuffer();
		
		int i = 0;
		int gogogo;
		try
		{
			while(true)
			{
				int termpos = i*4;
				int tid = ibf.get(i); i++;
				int len = ibf.get(i); i++;
				
				postingDict.put(tid, new Pair(termpos,len));
				
				for(gogogo = 0; gogogo<len; gogogo++) // go to next term id
					i++;
			}
		}
		catch(Exception e)
		{
			// reach end of binary file (nothing to worry...)
		}
		
		BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "term.dict")));
		for (String term : termDict.keySet()) {
			termWriter.write(term + "\t" + termDict.get(term) + "\n");
		}
		termWriter.close();

		BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "doc.dict")));
		for (String doc : docDict.keySet()) {
			docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
		}
		docWriter.close();

		BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
				outputDirname, "posting.dict")));
		for (Integer termId : postingDict.keySet()) {
			postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
					+ "\t" + postingDict.get(termId).getSecond() + "\n");
		}
		postWriter.close();
		
		return totalFileCount;
	}

	public static void deleteDir (File outdir) {
		if (outdir.isDirectory())
		{
			if(outdir.list().length==0) {
				outdir.delete();
			}else {
				File files[] = outdir.listFiles();
				
				for (File fileDelete : files)
                {
                    deleteDir(fileDelete);
                }
				if (outdir.list().length==0)
				{
					outdir.delete();
				}
			}
		}else {
			outdir.delete();
			System.out.println("File is deleted "+outdir.getAbsolutePath());
		}
	}

	public static void main(String[] args) throws IOException {
//		/* Parse command line */
//		if (args.length != 3) {
//			System.err
//					.println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
//			return;
//		}
//
//		/* Get index */
//		String className = "";
//		try {
//			className = args[0];
//		} catch (Exception e) {
//			System.err
//					.println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
//			throw new RuntimeException(e);
//		}
//
//		/* Get root directory */
//		String root = args[1];
//		
//
//		/* Get output directory */
//		String output = args[2];
//		runIndexer(className, root, output);
		P1Tester.testIndex("Basic", "./datasets/small", "./index/small");
	}

}
