package simpledb;

import sun.jvm.hotspot.ui.HeapParametersPanel;
import sun.net.www.HeaderParser;

import javax.xml.crypto.Data;
import java.awt.dnd.DropTarget;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc tupleDesc;
	private RandomAccessFile raf;
	
	/**
	 * Constructs a heap file backed by the specified file.
	 *
	 * @param f the file that stores the on-disk backing store for this heap
	 *          file.
	 */
	public HeapFile(File f, TupleDesc td) {
		// some code goes here
		this.file = f;
		this.tupleDesc = td;
		try {
			raf = new RandomAccessFile(f, "rw");
			assert raf.length() % BufferPool.getPageSize() == 0;
		} catch (IOException e) {
			throw new RuntimeException();
		}
	}
	
	/**
	 * Returns the File backing this HeapFile on disk.
	 *
	 * @return the File backing this HeapFile on disk.
	 */
	public File getFile() {
		// some code goes here
		return file;
	}
	
	/**
	 * Returns an ID uniquely identifying this HeapFile. Implementation note:
	 * you will need to generate this tableId somewhere ensure that each
	 * HeapFile has a "unique id," and that you always return the same value for
	 * a particular HeapFile. We suggest hashing the absolute file name of the
	 * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
	 *
	 * @return an ID uniquely identifying this HeapFile.
	 */
	public int getId() {
		// some code goes here
		return file.getAbsoluteFile().hashCode();
	}
	
	/**
	 * Returns the TupleDesc of the table stored in this DbFile.
	 *
	 * @return TupleDesc of this DbFile.
	 */
	public TupleDesc getTupleDesc() {
		// some code goes here
		return tupleDesc;
	}
	
	// see DbFile.java for javadocs
	public Page readPage(PageId pid) {
		// some code goes here
		
		// assert this because this is HeapFile, for sanity check
		assert pid instanceof HeapPageId;
		int pageNo = pid.pageNumber();
		int pageSize = BufferPool.getPageSize();
		
		// read from pageNo * pageSize -- (pageNo + 1) * pageSize
		// pageNo starts from 0
		byte[] pageData = new byte[pageSize];
		try {
			raf.seek(pageNo * pageSize);
			raf.read(pageData);
			return new HeapPage((HeapPageId) pid, pageData);
		} catch (IOException e) {
			throw new RuntimeException("HeapFile readPage IO exception");
		}
	}
	
	// see DbFile.java for javadocs
	public void writePage(Page page) throws IOException {
		// some code goes here
		// not necessary for lab1
		int pageNo = page.getId().pageNumber();
		int pageSize = BufferPool.getPageSize();
		
		raf.seek(pageNo * pageSize);
		raf.write(page.getPageData());
	}
	
	/**
	 * Returns the number of pages in this HeapFile.
	 */
	public int numPages() {
		// some code goes here
		try {
			assert raf.length() % BufferPool.getPageSize() == 0;
			return (int) (raf.length() / BufferPool.getPageSize());
		} catch (IOException e) {
			throw new RuntimeException("HeapFile numPages exception");
		}
	}
	
	private HeapPageId getEmptyPageNo(TransactionId tid) throws TransactionAbortedException, DbException, IOException {
		// search for an existing empty page.
		for (int pgNo = 0; pgNo < numPages(); ++pgNo) {
			HeapPageId pid = new HeapPageId(getId(), pgNo);
			HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
			if (page.getNumEmptySlots() > 0)
				return pid;
		}
		
		// if no page is empty, create another one and flush to the disk.
		HeapPageId newPid = new HeapPageId(getId(), numPages());
		HeapPage newPage = new HeapPage(newPid, HeapPage.createEmptyPageData());
		writePage(newPage);
		
		return newPid;
	}
	
	// see DbFile.java for javadocs
	public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		ArrayList<Page> dirtyPages = new ArrayList<>();
		
		// get a page with empty slot.
		HeapPageId pid = getEmptyPageNo(tid);
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		page.insertTuple(t);
		
		dirtyPages.add(page);
		
		return dirtyPages;
		// not necessary for lab1
	}
	
	// see DbFile.java for javadocs
	public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
			TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		ArrayList<Page> dirtyPages = new ArrayList<>();
		
		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
		page.deleteTuple(t);
		
		dirtyPages.add(page);
		
		return dirtyPages;
	}
	
	// see DbFile.java for javadocs
	public DbFileIterator iterator(TransactionId tid) {
		// some code goes here
		
		return new DbFileIterator() {
			private int curPageNo;
			private HeapPage curPage = null;
			private Iterator<Tuple> pageIter = null;
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				curPageNo = 0;
				HeapPageId pid = new HeapPageId(getId(), curPageNo);
				curPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
				pageIter = curPage.iterator();
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				// not opened yet
				if (pageIter == null || curPage == null)
					return false;
				
				// not reaches the end
				return !((curPageNo + 1) == numPages() && !pageIter.hasNext());
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if (!hasNext()) {
					throw new NoSuchElementException("invoke Iterator.next() upon end");
				}
				
				if (pageIter.hasNext()) {
					return pageIter.next();
				} else {
					curPageNo++;
					HeapPageId pid = new HeapPageId(getId(), curPageNo);
					curPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
					pageIter = curPage.iterator();
					assert pageIter.hasNext();
					// TODO : release the page
					return pageIter.next();
				}
			}
			
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				curPageNo = 0;
				HeapPageId pid = new HeapPageId(getId(), curPageNo);
				curPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, null);
				pageIter = curPage.iterator();
			}
			
			@Override
			public void close() {
				// TODO : release the page
				curPage = null;
				pageIter = null;
			}
		};
	}
	
}

