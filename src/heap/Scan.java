package heap;

import java.io.IOException;

import bufmgr.BufMgrException;
import bufmgr.BufferPoolExceededException;
import bufmgr.HashEntryNotFoundException;
import bufmgr.HashOperationException;
import bufmgr.InvalidFrameNumberException;
import bufmgr.PageNotReadException;
import bufmgr.PagePinnedException;
import bufmgr.PageUnpinnedException;
import bufmgr.ReplacerException;
import diskmgr.Page;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.Tuple;

public class Scan {

	private Heapfile heapFile;
	private RID currentRid = new RID(); 
	private boolean firstScaned = false;
	public Scan(Heapfile hf) throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException {
    
        Page tempPage = new Page();
		heapFile = hf;
		SystemDefs.JavabaseBM.pinPage(hf.getFirstPageID(),tempPage, false); 
	
		heapFile.getHfpage().openHFpage(tempPage);
		
        currentRid = hf.getHfpage().firstRecord();	
	}

	public Tuple getNext(RID rid) throws IOException, InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		// TODO Auto-generated method stub
		// heapFile.setCurPage(rid.pageNo);
		if(heapFile == null){
		    return null;
		}
		
	
//		System.out.println("page of rid "+rid.pageNo);
		Page tempPage = new Page();
		
		SystemDefs.JavabaseBM.pinPage(currentRid.pageNo, tempPage, false);
		heapFile.getHfpage().openHFpage(tempPage);
		SystemDefs.JavabaseBM.unpinPage(currentRid.pageNo, true);
        
        if(firstScaned== false){
		    firstScaned = true;
		    rid.copyRid(currentRid);
		    rid= currentRid;
		    if(currentRid==null)
		    return null;
		    return heapFile.getHfpage().getRecord(rid);
		}
        
		RID temp = heapFile.getHfpage().nextRecord(currentRid);
		
		if(temp==null){
			PageId pageId = new PageId();
			pageId.pid = heapFile.getHfpage().getNextPage().pid;
			
			if (pageId.pid != -1) {
				SystemDefs.JavabaseBM.pinPage(pageId, tempPage, false);
				heapFile.getHfpage().openHFpage(tempPage);
				SystemDefs.JavabaseBM.unpinPage(pageId, true);
				
				temp =  heapFile.getHfpage().firstRecord();	
			}
		}
		
//		System.out.println("scanned tuple id is "+temp);
		if (temp != null) {
			 rid.copyRid(temp);
			rid=temp;
			currentRid=temp;
			return heapFile.getHfpage().getRecord(temp);
		} else {
			closescan();
			rid=null;
			return null;
		}
	}

	public void closescan() {
		// TODO Auto-generated method stub
        if(heapFile == null){
		    return;
		}
		try {
			SystemDefs.JavabaseBM.unpinPage(heapFile.getFirstPageID(), true);
		} catch (ReplacerException | PageUnpinnedException | HashEntryNotFoundException | InvalidFrameNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		heapFile = null;
	}

	/**
	 * @return the firstPageID
	 */
	// public PageId getFirstPageID() {
	// return heapFile.getFirstPageID();
	// }

	/**
	 * @return the heapFile
	 */
	// public Heapfile getHeapFile() {
	// return heapFile;
	// }

	/**
	 * @param heapFile
	 *            the heapFile to set
	 */
	// public void setHeapFile(Heapfile heapFile) {
	// this.heapFile = heapFile;
	// }

}
