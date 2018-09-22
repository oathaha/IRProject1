/*
 * Mr. Komson Najard 5988020 Sec 1
 * Mr. Thanadon Bunkurd 5988073 Sec 1
 * Mr. Chanathip Pornprasit 5988179 Sec 1
 */

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
		//int tid = 0;
		List<Integer> docids = new ArrayList<Integer>();
		int a,tid,doclen;
		tid = 0;
		try
		{
			int i = 0;
			//System.out.println(pos + " " + fc.size());
			
			IntBuffer ib = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size()).asIntBuffer();
			int pos = (int)ib.get((int)fc.position()/4);
			//System.out.println("pos = " + pos);
			try
			{
				while(true)
				{
					tid = ib.get(i); i++;
					doclen = ib.get(i); i++;
					//System.out.println(tid + " " + pos);
					if(tid == pos) // found term id
					{
						//System.out.println("found yayyay");
						for(a=1; a<= doclen; a++)
						{
							docids.add(ib.get(i));
							i++;
						}
							
						break;
					}
					else // go to next term id
						while(doclen > 0) 
						{
							doclen--;
							i++;
						}
				}
			}
			catch(Exception e)
			{
				// reach end of file, nothing to do...
			}
//			tid = ib.get(pos/4);
//			pos+=4;
//			int doclen = ib.get(pos/4);
//			pos+=4;
			
//			for(a=pos/4; a<= doclen; a++)
//				docids.add(ib.get(a));
		} 
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("doc ids = " + docids);
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

