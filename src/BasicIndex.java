import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;


public class BasicIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		
		return null;
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) throws IOException {
		/*
		 * TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, fc.size(), (p.getList().size()+2)*4).asIntBuffer();
		ib.put(p.getTermId());
		ib.put(p.getList().size());
		for(Integer a: p.getList())
			ib.put(a);
	}
}

