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

import chainexception.ChainException;
import diskmgr.DiskMgrException;
import diskmgr.DuplicateEntryException;
import diskmgr.FileIOException;
import diskmgr.FileNameTooLongException;
import diskmgr.InvalidPageNumberException;
import diskmgr.InvalidRunSizeException;
import diskmgr.OutOfSpaceException;
import diskmgr.Page;
import global.GlobalConst;
import global.PageId;
import global.RID;
import global.SystemDefs;
import heap.HFPage;
import heap.InvalidSlotNumberException;
import heap.InvalidTupleSizeException;
import heap.Tuple;

public class Heapfile implements GlobalConst {
	private String hfName;
	private PageId firstPageID = new PageId();
	// private PageId lastPageId;
	private HFPage hfPage;
	int count = 0;

	public Heapfile(String string) throws IOException, FileIOException, InvalidPageNumberException, DiskMgrException, FileNameTooLongException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException {
		// TODO Auto-generated constructor stub
		Page tempPage = new Page();
		if (string == null || string.equals("")) {
			// produce a temp file here
		} else if (exist(string)) // file already exists
		{
			hfPage = new HFPage();
			setHfName(string);
			setFirstPageID(SystemDefs.JavabaseDB.get_file_entry(string));

			SystemDefs.JavabaseBM.pinPage(getFirstPageID(), tempPage, false);
			hfPage.openHFpage(tempPage);
			SystemDefs.JavabaseBM.unpinPage(getFirstPageID(), true);

			// open the file here
		} else {
			hfPage = new HFPage();
			setHfName(string);
			Page page = new Page();
			SystemDefs.JavabaseDB.add_file_entry(string, SystemDefs.JavabaseBM.newPage(page, 1));

			setFirstPageID(SystemDefs.JavabaseDB.get_file_entry(string));
			SystemDefs.JavabaseBM.unpinPage(getFirstPageID(), false);
			hfPage.init(getFirstPageID(), page);
			SystemDefs.JavabaseBM.pinPage(getFirstPageID(), tempPage, false);
			hfPage.openHFpage(tempPage);
			SystemDefs.JavabaseBM.unpinPage(getFirstPageID(), true);
			// hfPage.setCurPage(getFirstPageID());

			// lastPageId = SystemDefs.JavabaseDB.get_file_entry(string);
			// create an empty file
		}
	}

	private boolean exist(String string) throws FileIOException, InvalidPageNumberException, DiskMgrException, IOException { // check
		// method
		// stub
		if (SystemDefs.JavabaseDB.get_file_entry(string) != null) {
			return true;
		}
		return false;
	}

	public RID insertRecord(byte[] recPtr) throws ChainException, IOException {
		// TODO Auto-generated method stub
		if (recPtr.length == 0) {
			throw new InvalidTupleSizeException(new Exception(), "Heapfile.java: insertRecord() falied, invalid tuple size");
		}

		// Throws: InvalidTupleSizeException
		// invalid tuple size
		if (MINIBASE_PAGESIZE < recPtr.length) {
//			System.out.println("exception");
			throw new SpaceNotAvailableException(new Exception(), "Heapfile.java: insertRecord() falied, invalid tuple size");
		}
		int prvPID = -500;
		int nxtPID = -400;
		PageId pageId = new PageId();
		pageId.pid = getFirstPageID().pid;
		// hfPage.setCurPage(pageId);
		Page tempPage = new Page();
		SystemDefs.JavabaseBM.pinPage(getFirstPageID(), tempPage, false);
		hfPage.openHFpage(tempPage);
		SystemDefs.JavabaseBM.unpinPage(getFirstPageID(), true);
		// System.out.println(pageId);
//		System.out.println(pageId.pid);
		while (pageId.pid >= 0 && prvPID != nxtPID) {
			// System.out.println(pageId.pid);
			// hfPage.setCurPage(pageId);
			SystemDefs.JavabaseBM.pinPage(pageId, tempPage, false);
			hfPage.openHFpage(tempPage);
			SystemDefs.JavabaseBM.unpinPage(pageId, true);
			if (hfPage.available_space() < recPtr.length) {
				try {
					prvPID = pageId.pid;
					pageId.pid = hfPage.getNextPage().pid;
//					System.out.println(pageId.pid);
					nxtPID = pageId.pid;
					// System.out.println(pageId);

				} catch (Exception e) {
					break;
				}

			} else {
				count++;
				// System.out.println(count);
				// System.out.println("page inserted");
//				System.out.println("-----------------------------------------------------------------------------------");
				return hfPage.insertRecord(recPtr);

			}

		}
//		System.out.println("-----------------------------------------------------------------------------------");

		Page page = new Page();
		// HFPage hfp = new HFPage();
		// Throws: SpaceNotAvailableException
		// no space left
//		System.out.println("current page is " + hfPage.getCurPage().pid);
		pageId.pid = hfPage.getCurPage().pid;
//		System.out.println("pageID " + pageId);
		PageId newPageId = new PageId();
		newPageId.pid = SystemDefs.JavabaseBM.newPage(page, 1).pid;
		SystemDefs.JavabaseBM.unpinPage(newPageId, true);
//		System.out.println("pageID " + pageId);
		hfPage.setNextPage(newPageId);
//		System.out.println("pageID " + pageId);
//		System.out.println("set page " + hfPage.getCurPage().pid + " next to be " + newPageId.pid);
//		System.out.println("pageID " + pageId);
		hfPage.init(newPageId, page);
		SystemDefs.JavabaseBM.pinPage(newPageId, tempPage, false);
		hfPage.openHFpage(tempPage);
		SystemDefs.JavabaseBM.unpinPage(newPageId, true);
//		System.out.println("pageID here " + pageId);
		// hfPage.setCurPage(newPageId);
		hfPage.setPrevPage(pageId);
//		System.out.println("pageID " + pageId);
//		System.out.println("set page " + hfPage.getCurPage().pid + " prev to be " + pageId.pid);
//		System.out.println("pageID " + pageId);
		// hfp.setPrevPage(pageId);

		// pageId = hfPage.getCurPage();
		// hfPage.init(newPageId, page);
		// // firstPageID=newPageId;
		// //System.out.println(newPageId);
		// hfPage.setNextPage(newPageId);
		// //System.out.println(hfPage.getNextPage());
		// hfPage.setCurPage(newPageId);
		// hfPage.setPrevPage(pageId);
		count++;
		return hfPage.insertRecord(recPtr);

		// System.out.println(count);
		// System.out.println("page inserted");

		// Throws: InvalidSlotNumberException
		// invalid slot number

		// Throws: HFException
		// heapfile exception

		// Throws: HFBufMgrException
		// exception thrown from bufmgr layer

		// Throws: HFDiskMgrException
		// exception thrown from diskmgr layer

		// Throws: IOException
		// I/O errors

		// Returns:
		// the rid of the record

	}

	public int getRecCnt() throws IOException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		// TODO Auto-generated method stub
		int counter = 0;
		Page tempPage = new Page();
		PageId pageId = getFirstPageID();
		// SystemDefs.JavabaseBM.pinPage(pageId, tempPage, false);
		// hfPage.openHFpage(tempPage);
		// SystemDefs.JavabaseBM.unpinPage(pageId, true);
		RID rid = new RID();
		// RID rid = hfPage.firstRecord();
		int prvPID = -500;
		int nxtPID = -400;
		while (pageId.pid >= 0 && prvPID != nxtPID) {
			SystemDefs.JavabaseBM.pinPage(pageId, tempPage, false);
			hfPage.openHFpage(tempPage);
			SystemDefs.JavabaseBM.unpinPage(pageId, true);
			rid = hfPage.firstRecord();
			while (rid != null) {
				counter++;
				// System.out.println("page counted");
				rid = hfPage.nextRecord(rid);
			}

			prvPID = pageId.pid;
			pageId.pid = hfPage.getNextPage().pid;
			nxtPID = pageId.pid;
			// System.out.println(nxtPID);

		}

		return counter;
		// Return number of records in file.
		// Throws: InvalidSlotNumberException
		// invalid slot number
		// Throws: InvalidTupleSizeException
		// invalid tuple size
		// Throws: HFBufMgrException
		// exception thrown from bufmgr layer
		// Throws: HFDiskMgrException
		// exception thrown from diskmgr layer
		// Throws: IOException
		// I/O errors
	}

	public Scan openScan() throws ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, IOException {
		// TODO Auto-generated method stub
		Scan scan = new Scan(this);
		return scan;
	}

	public boolean deleteRecord(RID rid) throws InvalidSlotNumberException, IOException, ChainException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException {
		// TODO Auto-generated method stub
		Tuple tuple = getRecord(rid);
		if (tuple != null) {
			PageId pid = rid.pageNo;
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pid, page, false);
			HFPage hfp = new HFPage(page);
			hfp.deleteRecord(rid);
			SystemDefs.JavabaseBM.unpinPage(pid, true);
			return true;
		}
		return false;
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws InvalidUpdateException, IOException, InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		// TODO Auto-generated method stub

		Tuple oldTuple = getRecord(rid);

		if (oldTuple == null) {
			return false;
//			 throw new InvalidUpdateException(new ChainException(),
//			 "Heapfile.java: updateRecord() falied, record not found");
		}
//		System.out.println(rid.slotNo);
//		System.out.println("page of rid " + rid.pageNo);
//		System.out.println("old tuple " + oldTuple.getLength());
//		System.out.println("new tuple " + newTuple.getLength());
		// if (newTuple.getLength() == 0) {
		// throw new InvalidUpdateException(new Exception(),
		// "Heapfile.java: updateRecord() falied, record not found");
		//
		// }

		if (newTuple.getLength() == oldTuple.getLength()) {
			oldTuple.tupleCopy(newTuple);
			// oldTuple.tupleInit(newTuple.getTupleByteArray(),
			// oldTuple.getOffset(), newTuple.getLength());
//			System.out.println("updated");
			return true;
		}

//		System.out.println("exception");
		
		throw new InvalidUpdateException(new Exception(), "Heapfile.java: updateRecord() falied, record lenght not correct");

		// deleteRecord(rid);
		// hfPage.insertRecord(newTuple.getTupleByteArray());

		// return true;

		// If updated records grow they
		// may require to be relocated (moved) to a new empty space, else if the
		// updated
		// records shrank, you should consider the empty space resulted as new
		// slot when
		// inserting new records.
		// Returns:
		// true:update success false: can't find the record
		// Throws: InvalidSlotNumberException
		// invalid slot number
		// Throws: InvalidUpdateException
		// invalid update on record
		// Throws: InvalidTupleSizeException
		// invalid tuple size
		// Throws: HFException
		// heapfile exception
		// Throws: HFBufMgrException
		// exception thrown from bufmgr layer
		// Throws: HFDiskMgrException
		// exception thrown from diskmgr layer
		// Throws: Exception
		// other exception
	}

	public Tuple getRecord(RID rid) throws IOException, InvalidSlotNumberException, ReplacerException, HashOperationException, PageUnpinnedException, InvalidFrameNumberException, PageNotReadException, BufferPoolExceededException, PagePinnedException, BufMgrException, HashEntryNotFoundException {
		// TODO Auto-generated method stub
		if (rid == null) {
			return null;
		}
		Page tempPage = new Page();
		PageId pageId = new PageId();
		pageId.pid = rid.pageNo.pid;

		SystemDefs.JavabaseBM.pinPage(pageId, tempPage, false);
		hfPage.openHFpage(tempPage);
		SystemDefs.JavabaseBM.unpinPage(pageId, true);

		Tuple tuple = new Tuple();
		// tuple=hfPage.getRecord(rid);
		tuple = hfPage.returnRecord(rid);
		return tuple;
	}

	/**
	 * @return the firstPageID
	 */
	public PageId getFirstPageID() {
		PageId temp = new PageId();
		temp.pid = firstPageID.pid;
		return temp;
	}

	/**
	 * @param firstPageID
	 *            the firstPageID to set
	 */
	public void setFirstPageID(PageId firstPageID) {
		this.firstPageID.pid = firstPageID.pid;
	}

	/**
	 * @return the hfName
	 */
	public String getHfName() {
		return hfName;
	}

	/**
	 * @param hfName
	 *            the hfName to set
	 */
	public void setHfName(String hfName) {
		this.hfName = hfName;
	}

	// /**
	// * @return the lastPageId
	// */
	// public PageId getLastPageId() {
	// return lastPageId;
	// }
	//
	// /**
	// * @param lastPageId the lastPageId to set
	// */
	// public void setLastPageId(PageId lastPageId) {
	// this.lastPageId = lastPageId;
	// }

	/**
	 * @return the hfpage
	 */
	public HFPage getHfpage() {
		return hfPage;
	}

	/**
	 * @param hfpage
	 *            the hfpage to set
	 */
	public void setHfpage(HFPage hfpage) {
		this.hfPage = hfpage;
	}

}
