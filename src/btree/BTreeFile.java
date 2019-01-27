/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;
/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used
	 * to drive a visualization tool that shows the inner workings of the b-tree
	 * during its operations.
	 *
	 * @param filename
	 *            input parameter. The trace file name
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this
	 *         btree file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno)
			throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 *
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException,
			PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; -
		 * headerPage, headerPageId valid and pinned - dbname contains a copy of
		 * the name of the database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException,
			IteratorException, PinPageException, ConstructPageException,
			UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page,
					headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
					.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException,
			PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */
	public void insert(KeyClass key, RID rid) throws KeyTooLongException,
			KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException,
			UnpinPageException, PinPageException, NodeNotMatchException,
			ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException,
			IOException

	{
		KeyDataEntry newRootEntry;
		if(headerPage.get_rootId().pid==INVALID_PAGE)
		{
			BTLeafPage leafPage;
			PageId root;
			//creating a new leaf page
			leafPage = new BTLeafPage(headerPage.get_keyType());
			//root is set as the leaf page(current page)
			root = leafPage.getCurPage();
			//header pointing to the root
			updateHeader(root);
			//setting left and right links to invalid page
			leafPage.setNextPage(new PageId(INVALID_PAGE));
			leafPage.setPrevPage(new PageId(INVALID_PAGE));
			//inserting the record
			leafPage.insertRecord(key, rid);
			unpinPage(root, true);
			return;	
		}
		else
		{
			newRootEntry=_insert(key, rid, headerPage.get_rootId());
			
			if(newRootEntry != null)
			{
				//creating a new Root Pagebecause split has occured 
				BTIndexPage newRootPage = new BTIndexPage(headerPage.get_keyType());
				PageId newRootPageId = newRootPage.getCurPage();
				
				//inserting the key into the new root node
				newRootPage.insertKey( newRootEntry.key, ((IndexData)newRootEntry.data).getData() );
				//setting the previous pointer
				newRootPage.setPrevPage(headerPage.get_rootId());
				
				unpinPage(newRootPageId, true);
				updateHeader(newRootPageId);
				
			}
		}
		
		
		
		
		
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException,
			LeafDeleteException, ConstructPageException, DeleteRecException,
			IndexSearchException, UnpinPageException, LeafInsertRecException,
			ConvertException, IteratorException, IndexInsertRecException,
			KeyNotMatchException, NodeNotMatchException, InsertException

	{
		Page page;
		BTSortedPage currentPage;
		KeyDataEntry upEntry;
		
		page = pinPage(currentPageId);
		//.get_keyType() to get the type of data stored in the header.
		currentPage = new BTSortedPage(page, headerPage.get_keyType());
		
		if(currentPage.getType()==NodeType.INDEX) 
		{
			//used to store the index page which is passed 
			BTIndexPage currentIndexPage = new BTIndexPage(page, headerPage.get_keyType());
			//used to store the PageId of the current page
			PageId currentIndexPageId = currentPageId  ;
			//Used to traverse to the next node 
			PageId nextPageId = currentIndexPage.getPageNoByKey(key);
			//We are unpinning the current index page
			unpinPage(currentIndexPageId);
			
			//We use upEntry to store the value which is pushed up
			//We are using a recursive function call in order to reach the leaf node
			upEntry = _insert(key, rid, nextPageId);
			
			//Here we are basically returning the value null if there is no split in the lower layers (upEntry == null) . 
			//If split has occurred we are passing the value using upEntry hence upEntry!= NULL
			if(upEntry == null)
			{
				return null;			
			}
			
			//Pinning the Page
			pinPage(currentPageId);
			
			//we are again initializing the current index page
			currentIndexPage = new BTIndexPage(currentPageId, headerPage.get_keyType());
			
			//If space is available in the index page we are inserting the key and returning null
			if(currentIndexPage.available_space() >= BT.getKeyDataLength( upEntry.key, NodeType.INDEX))
			{
				currentIndexPage.insertKey(upEntry.key, ((IndexData)upEntry.data).getData());
				unpinPage(currentIndexPageId, true);
				return null;		
			}
			
			// Now if there is no space we have to create a new index node and a new 
			//index page id and redistribute the entries
			
			BTIndexPage newIndexPage;
			PageId newIndexPageId;
			
			//initializing the objects
			
			newIndexPage = new BTIndexPage(headerPage.get_keyType());
			newIndexPageId = newIndexPage.getCurPage();
			
			KeyDataEntry tmpKeyDataEntry;
			PageId tmpPageId;
			RID delRid = new RID();
			
			for(tmpKeyDataEntry = currentIndexPage.getFirst(delRid); tmpKeyDataEntry != null; 
				tmpKeyDataEntry = currentIndexPage.getFirst(delRid))
			{
				//Here we are inserting the data from current index page to the new index page
				newIndexPage.insertKey(tmpKeyDataEntry.key, ((IndexData) tmpKeyDataEntry.data).getData());
				//We use delete Sorted Record because we are always deleting the first element
				currentIndexPage.deleteSortedRecord(delRid);					
			}
			
			//used for storing the returned value of .getFirst()
			RID insRid = new RID();
			//used to store the data of the last moved record in order to undo it in the future
			KeyDataEntry undoEntry = null;
			
			for(tmpKeyDataEntry = newIndexPage.getFirst(insRid); currentIndexPage.available_space() > newIndexPage.available_space();
				tmpKeyDataEntry = newIndexPage.getFirst(insRid)) 
			
			{
				//here we are assigning the undo entry and the required data will be initialized in the last iteration of the
				//for loop
				undoEntry = tmpKeyDataEntry;
				//we are inserting data in the current index page
				currentIndexPage.insertKey(tmpKeyDataEntry.key , ((IndexData) tmpKeyDataEntry.data).getData());
				//here we are deleting the records which we are transferring from new to current
				newIndexPage.deleteSortedRecord(insRid);	
			}
			
			//to get the slot number of the last inserted record
			int undo_slotNumber = (int) currentIndexPage.getSlotCnt()-1;
			//to know which rid to delete from the current index page
			RID undo_rid = new RID(currentIndexPage.getCurPage(), undo_slotNumber);
			
			if(currentIndexPage.available_space() < newIndexPage.available_space())
			{
				//Here we are inserting the undo entry
				newIndexPage.insertKey(undoEntry.key, ((IndexData) upEntry.data).getData());
				//Here we are deleting the entry from the index node
				currentIndexPage.deleteSortedRecord(undo_rid);	
			}
			
			tmpKeyDataEntry = newIndexPage.getFirst(insRid);
			//We are comparing the upentry key and the first key of the new index page
			// if >=0 upentry key is greater than new index page key
			// if <0 upentry key is smaller than new index page key
			//if == upentry key is equal to new index page key
			if(BT.keyCompare(upEntry.key, tmpKeyDataEntry.key)>=0)
			{
				newIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
			}
			else
			{
				currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());	
			}
			
			unpinPage(currentIndexPageId, true);
			
			//filling up entry so that it can be passed up
			upEntry = newIndexPage.getFirst(delRid);
			
			//we are setting the previous page of the index to the updated upentry
			newIndexPage.setPrevPage(((IndexData) upEntry.data).getData());
			
			//deleting the first entry as we are giving it up
			newIndexPage.deleteSortedRecord(delRid);
			
			unpinPage(newIndexPageId, true);
			//we need to update the right link of the upentry 
			((IndexData)upEntry.data).setData(newIndexPageId);
			
			//returning up entry
			return upEntry;
			
		}
		
		else if(currentPage.getType() == NodeType.LEAF)
		{
			
			//instance for a new leaf node
			BTLeafPage currentLeafPage = new BTLeafPage(page, headerPage.get_keyType());
			//initializing the cuurentPageId to the current leaf page
			PageId currentLeafPageId = currentPageId;
			
			if(currentLeafPage.available_space() >= BT.getKeyDataLength(key, NodeType.LEAF))
			{
				//System.out.println("loop2");
				currentLeafPage.insertRecord(key, rid);
				unpinPage(currentLeafPageId, true);	
				return null;
			}
			//3tem.out.println("working");
			//We are creating a new leaf node for splitting as space is not available at the current page. 
			BTLeafPage newLeafPage = new BTLeafPage(headerPage.get_keyType());
			PageId newLeafPageId = newLeafPage.getCurPage();
			
			//for storing the data when the transferring of keys occur
			KeyDataEntry tmpKeyDataEntryLeaf;
			//RID
			RID insLeafRid = new RID();
			RID delLeafRid = new RID();
			
			//setting the new leaf page link to next page of the current leaf page
			newLeafPage.setNextPage(currentLeafPage.getNextPage());
			//setting the link of the new leaf page to the previous page
			newLeafPage.setPrevPage(currentLeafPageId);
			//setting the link of the next page of the current page to the new leaf page
			currentLeafPage.setNextPage(newLeafPageId);
			
			//transferring data from current leaf node to the new leaf node
			for(tmpKeyDataEntryLeaf = currentLeafPage.getFirst(delLeafRid); tmpKeyDataEntryLeaf != null;
				tmpKeyDataEntryLeaf = currentLeafPage.getFirst(delLeafRid))
			{
				//inserting the keys to the new Leaf Node
				newLeafPage.insertRecord(tmpKeyDataEntryLeaf.key, ((LeafData) (tmpKeyDataEntryLeaf.data)).getData());
				
				//delete the keys from the current leaf node
				currentLeafPage.deleteSortedRecord(delLeafRid);					
			}
			
			KeyDataEntry undoLeaf = null;
			
			//Balancing the nodes
			
			for(tmpKeyDataEntryLeaf = newLeafPage.getFirst(insLeafRid); newLeafPage.available_space() < currentLeafPage.available_space();
				tmpKeyDataEntryLeaf = newLeafPage.getFirst(insLeafRid) )
			{
				//We use undoLeaf to store the last key transferred from new to current
				undoLeaf = tmpKeyDataEntryLeaf;
				//Here we are inserting data in current leaf node

				currentLeafPage.insertRecord(tmpKeyDataEntryLeaf.key, ((LeafData)(tmpKeyDataEntryLeaf.data)).getData());
				//Here we are deleting the key form the new leaf node
				newLeafPage.deleteSortedRecord(insLeafRid);
			}
			
			//cSlot is the last value of the current leaf page
			int cSlot = (int) currentLeafPage.getSlotCnt() - 1;
			//rid of the last element
			RID cRid = new RID(currentLeafPage.getCurPage(), cSlot);
			
			
			//Undoing the last undo
			if(BT.keyCompare(key,undoLeaf.key)<0)
			{
				if(currentLeafPage.available_space()<newLeafPage.available_space())
				{
					newLeafPage.insertRecord(undoLeaf.key, (((LeafData) undoLeaf.data).getData()));
					currentLeafPage.deleteSortedRecord(cRid);
					
				}
			}
			
			//if the key is greater than the undo leaf key then it is inserted in the new leaf page else 
			//it is inserted in the current leaf node
			if(BT.keyCompare(key,undoLeaf.key)>=0)
			{
				newLeafPage.insertRecord(key,rid);
			}
			else
			{
				currentLeafPage.insertRecord(key,rid);
			}
		
			//un pinning the page for other processes to use
			unpinPage(currentLeafPageId);
			//filling up upEntry to pass up to the index
			upEntry = new KeyDataEntry(newLeafPage.getFirst(delLeafRid).key, newLeafPageId);
			
			return upEntry;		
		}
	return null;	
	}

	



	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 *
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid) throws DeleteFashionException,
			LeafRedistributeException, RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException,
			IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException,
			IOException {
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			{
			return NaiveDelete(key, rid);
			}
		else
			throw new DeleteFashionException(null, "");
	}

	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key
	 * is null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text
	 * (for the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way
	 * left if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
	 */

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException,
			IteratorException, KeyNotMatchException, ConstructPageException,
			PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null
					&& BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno),
						headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necesary, to
	 * find the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException, DeleteRecException {
	
			BTLeafPage leafPage;
			//Iterator
			RID crid = new RID();
			RID nrid = new RID();
			KeyDataEntry entry;
			KeyDataEntry tmp;
			PageId nextPage;
			
			
		
			//returns the first leaf page
			leafPage = findRunStart(key, crid );
			
			//If there is no leaf Page
			if(leafPage == null)
				return false;
			
			
			//This is used to check wether the current leaf node reached is null or not
			entry = leafPage.getCurrent(crid);
			
			while(true)	
			{
				
				//To traverse to the next leaf page if the leaf is null and to return false
				//if event the corresponding pages are null
				while(entry == null)
					
				{
					
					//We are traversing to the next leaf page
					nextPage = leafPage.getNextPage();
					unpinPage(leafPage.getCurPage());
					//If we reach the end of the leaf pages then we return false
					if(nextPage.pid==INVALID_PAGE)
						 return false;
					 
					 leafPage = new BTLeafPage(pinPage(nextPage),headerPage.get_keyType());
					 //updating the entry to the new page
					 entry = leafPage.getFirst(nrid);	
				}
				//if the key is greater than the entry then no delete
				if(BT.keyCompare(key, entry.key)>0)
				{
					break;
				}
				
				//we are traversing records in the leaf page
				for(tmp = leafPage.getFirst(nrid); tmp != null ;tmp = leafPage.getNext(nrid))
				{
					
					
					if(BT.keyCompare(tmp.key, key)==0)
						{
						
						//Here we are deleting the entry
						leafPage.deleteSortedRecord(nrid);
						unpinPage(leafPage.getCurPage());
						return true;
						}
				}
				//we are traversing to the next leaf page if the key is not found
				//in the current leaf page
				nextPage = leafPage.getNextPage();
				
				//If the next page is invalid then we have reached the end of the leaf pages 
				// and the key is not found
				if(nextPage.pid == INVALID_PAGE )
					break;
				unpinPage(leafPage.getCurPage());
				leafPage = new BTLeafPage(nextPage ,headerPage.get_keyType());
				pinPage(leafPage.getCurPage());
				//updating the new entry
				entry = leafPage.getFirst(crid);
					
			}
			
			unpinPage(leafPage.getCurPage());
			return false;
	}
	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index (2) lo_key = null, hi_key!= null range scan from min
	 * to the hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key
	 * to max (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (
	 * might not unique) (5) lo_key!= null, hi_key!= null, lo_key < hi_key range
	 * scan from lo_key to hi_key
	 *
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws IOException, KeyNotMatchException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id) throws IOException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
