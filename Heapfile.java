package implementation;

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
	private PageId firstPageID;
	// private PageId lastPageId;
	private HFPage hfPage;
	int inserted = 0;
	int pagesCreated = 0;
	int pagesFound = 0;

	public Heapfile(String string) throws IOException, FileIOException, InvalidPageNumberException, DiskMgrException, FileNameTooLongException, InvalidRunSizeException, DuplicateEntryException, OutOfSpaceException, BufferPoolExceededException, HashOperationException, ReplacerException, HashEntryNotFoundException, InvalidFrameNumberException, PagePinnedException, PageUnpinnedException, PageNotReadException, BufMgrException {
		// TODO Auto-generated constructor stub
		if (string == null || string.equals("")) {
			// produce a temp file here
		} else if (exist(string)) // file already exists
		{
			hfPage = new HFPage();
			setHfName(string);
			setFirstPageID(SystemDefs.JavabaseDB.get_file_entry(string));
			hfPage.setCurPage(firstPageID);

			// open the file here
		} else {
			hfPage = new HFPage();
			setHfName(string);
			SystemDefs.JavabaseDB.add_file_entry(string, SystemDefs.JavabaseBM.newPage(new Page(), 1));
			setFirstPageID(SystemDefs.JavabaseDB.get_file_entry(string));
			hfPage.setCurPage(firstPageID);
			SystemDefs.JavabaseBM.unpinPage(firstPageID, false);
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

			throw new InvalidTupleSizeException(new Exception(), "Heapfile.java: insertRecord() falied, invalid tuple size");
		}
		
		
		PageId pageId = firstPageID;
		// System.out.println(pageId);
		int prvPID = -500;
		int nxtPID = -400;
		while (prvPID != nxtPID) {
			System.out.println("pageid " + pageId.pid);
			hfPage.setCurPage(pageId);
			hfPage.dumpPage();
			System.out.println("available space "+hfPage.available_space() );
			if (hfPage.available_space() < recPtr.length) {
				try {
					prvPID = pageId.pid;
					pageId = hfPage.getNextPage();
					nxtPID = pageId.pid;
					// System.out.println(pageId);

				} catch (Exception e) {
					break;
				}

			} else {
				System.out.println("inserted");
				inserted++;
				return hfPage.insertRecord(recPtr);

			}

		}

		if (hfPage.available_space() < recPtr.length) {
			Page page = new Page();
			// Throws: SpaceNotAvailableException
			// no space left
			PageId newPageId = SystemDefs.JavabaseBM.newPage(page, 1);
			SystemDefs.JavabaseBM.unpinPage(newPageId, true);

			pageId = hfPage.getCurPage();
			hfPage.init(newPageId, page);
			// firstPageID=newPageId;
			hfPage.setNextPage(newPageId);
			hfPage.setCurPage(newPageId);
			hfPage.setPrevPage(pageId);
		}

System.out.println("created new then inserted");
		inserted++;
		return hfPage.insertRecord(recPtr);

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

	public int getRecCnt() throws IOException {
		// TODO Auto-generated method stub
		System.out.println("inserted " + inserted);
		int counter = 0;
		PageId pageId = firstPageID;
		hfPage.setCurPage(pageId);
		RID rid = hfPage.firstRecord();

		int prvPID = -500;
		int nxtPID = -400;
		while (pageId.pid > 0 && prvPID != nxtPID) {

			while (rid != null) {
				counter++;
				// System.out.println("page counted");
				rid = hfPage.nextRecord(rid);
			}
			try {
				prvPID = pageId.pid;
				pageId = hfPage.getNextPage();
				nxtPID = pageId.pid;
				// System.out.println(nxtPID);
				hfPage.setCurPage(pageId);
			} catch (Exception e) {
				break;
			}
		}
		System.out.println("page counted " + counter);
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
			hfPage.setCurPage(pid);
			SystemDefs.JavabaseBM.pinPage(pid, new Page(), false);
			hfPage.deleteRecord(rid);
			SystemDefs.JavabaseBM.unpinPage(pid, true);
			return true;
		}
		return false;
	}

	public boolean updateRecord(RID rid, Tuple newTuple) throws ChainException, IOException {
		// TODO Auto-generated method stub

		Tuple oldTuple = getRecord(rid);
		if (oldTuple == null) {
			return false;
			// throw new InvalidUpdateException(new Exception(),
			// "Heapfile.java: updateRecord() falied, record not found");
		}
		if (newTuple.getLength() == 0) {
			throw new InvalidUpdateException(new Exception(), "Heapfile.java: updateRecord() falied, record not found");

		}

		if (newTuple.getLength() <= oldTuple.getLength()) {
			oldTuple.tupleSet(newTuple.getTupleByteArray(), oldTuple.getOffset(), newTuple.getLength());
			return true;
		}

		// throw new InvalidTupleSizeException(new Exception(),
		// "Heapfile.java: updateRecord() falied, tupple size is not correct");

		deleteRecord(rid);
		hfPage.insertRecord(newTuple.getTupleByteArray());

		return true;
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

	public Tuple getRecord(RID rid) throws IOException, InvalidSlotNumberException {
		// TODO Auto-generated method stub
		Tuple tuple = null;
		PageId pageId = new PageId();
		pageId = hfPage.firstRecord().pageNo;

		while (pageId != null) {

			tuple = hfPage.getRecord(rid);

			if (tuple == null) {
				try {
					pageId = hfPage.getNextPage();
					hfPage.setCurPage(pageId);
				} catch (Exception e) {
					break;
				}
			}
		}

		return tuple;
	}

	/**
	 * @return the firstPageID
	 */
	public PageId getFirstPageID() {
		return firstPageID;
	}

	/**
	 * @param firstPageID
	 *            the firstPageID to set
	 */
	public void setFirstPageID(PageId firstPageID) {
		this.firstPageID = firstPageID;
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
