import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;


public class BasicIndex implements BaseIndex {

	public PostingList readPosting(FileChannel fc) {
		/*
		 * TODO: Your code here
		 *       Read and return the postings list from the given file.
		 */
		int tid = 0;
		List<Integer> docids = new ArrayList<Integer>();
		try
		{
			int a;
			int pos = (int)fc.position();
			IntBuffer ib = fc.map(FileChannel.MapMode.READ_WRITE, pos, fc.size()).asIntBuffer();
			tid = ib.get(pos/4);
			pos+=4;
			int doclen = ib.get(pos/4);
			pos+=4;
			
			for(a=pos/4; a<= doclen; a++)
				docids.add(ib.get(a));
		} 
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new PostingList(tid,docids);
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p){
		/*
		 * TODO: Your code here
		 *       Write the given postings list to the given file.
		 */
		IntBuffer ib = null;
		try
		{
			ib = fc.map(FileChannel.MapMode.READ_WRITE, fc.size(), (p.getList().size()+2)*4).asIntBuffer();
		} catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ib.put(p.getTermId());
		ib.put(p.getList().size());
		for(Integer a: p.getList())
			ib.put(a);
	}
}

