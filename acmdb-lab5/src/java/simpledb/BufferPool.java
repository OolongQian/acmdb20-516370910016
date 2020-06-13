package simpledb;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking; when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
	/**
	 * Bytes per page, including header.
	 */
	private static final int PAGE_SIZE = 4096;
	
	private static int pageSize = PAGE_SIZE;
	
	/**
	 * Default number of pages passed to the constructor. This is used by
	 * other classes. BufferPool should use the numPages argument to the
	 * constructor instead.
	 */
	public static final int DEFAULT_PAGES = 50;
	
	/**
	 * pageBuffer works as cache.
	 * We implement least recently used eviction policy,
	 * thus need to maintain an recently used ordering in this buffer.
	 * The most recently used page is lifted to front.
	 */
	private int numPages;
	
	private static ConcurrentHashMap<PageId, Page> pid2page;
	private static TransactionManager transactionManager;
	
	/**
	 * Creates a BufferPool that caches up to numPages pages.
	 *
	 * @param numPages maximum number of pages in this buffer pool.
	 */
	public BufferPool(int numPages) {
		// some code goes here
		this.numPages = numPages;
		pid2page = new ConcurrentHashMap<>();
		transactionManager = new TransactionManager(this);
	}
	
	public static int getPageSize() {
		return pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void setPageSize(int pageSize) {
		BufferPool.pageSize = pageSize;
	}
	
	// THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
	public static void resetPageSize() {
		BufferPool.pageSize = PAGE_SIZE;
	}
	
	/**
	 * Retrieve the specified page with the associated permissions.
	 * Will acquire a lock and may block if that lock is held by another
	 * transaction.
	 * <p>
	 * The retrieved page should be looked up in the buffer pool.  If it
	 * is present, it should be returned.  If it is not present, it should
	 * be added to the buffer pool and returned.  If there is insufficient
	 * space in the buffer pool, an page should be evicted and the new page
	 * should be added in its place.
	 *
	 * @param tid  the ID of the transaction requesting the page
	 * @param pid  the ID of the requested page
	 * @param perm the requested permissions on the page
	 */
	public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
			throws TransactionAbortedException, DbException {
		
		transactionManager.acquireLock(tid, pid, perm);
		
		// Get Page
//		Page page = pid2page.get(pid);
//		if (page == null) {
//			if (pid2page.size() >= numPages)
//				evictPage();
//
//			page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
//			pid2page.put(pid, page);
//		}
//
//		return page;
		
		if (!pid2page.containsKey(pid)) {
			if (pid2page.size() >= numPages)
				evictPage();

			Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
			pid2page.put(pid, page);
		}
		
		return pid2page.get(pid);
	}
	
	/**
	 * ensure the page is in buffer pool. */
	private void pin2pool(Page page) throws DbException {
		PageId pid = page.getId();
		if (!pid2page.containsKey(pid) && pid2page.size() >= numPages)
			evictPage();
		pid2page.put(pid, page);
	}
	
	/**
	 * Releases the lock on a page.
	 * Calling this is very risky, and may result in wrong behavior. Think hard
	 * about who needs to call this and why, and why they can run the risk of
	 * calling it.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 * @param pid the ID of the page to unlock
	 */
	public synchronized void releasePage(TransactionId tid, PageId pid) {
		// some code goes here
		// not necessary for lab1|lab2
		transactionManager.release(tid, pid);
	}
	
	/**
	 * Release all locks associated with a given transaction.
	 *
	 * @param tid the ID of the transaction requesting the unlock
	 */
	public void transactionComplete(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		transactionComplete(tid, true);
	}
	
	/**
	 * Return true if the specified transaction has a lock on the specified page
	 */
	public boolean holdsLock(TransactionId tid, PageId p) {
		// some code goes here
		// not necessary for lab1|lab2
		return transactionManager.holdsLock(tid, p);
	}
	
	/**
	 * Commit or abort a given transaction; release all locks associated to
	 * the transaction.
	 *
	 * @param tid    the ID of the transaction requesting the unlock
	 * @param commit a flag indicating whether we should commit or abort
	 */
	public synchronized void transactionComplete(TransactionId tid, boolean commit)
			throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		if (commit)
			flushPages(tid);
		else { // this tid is aborted.
			// discard all dirty pages(if is read/write)
			List<TransactionManager.Lock> locks = transactionManager.getLocksFromTid(tid);
			for (TransactionManager.Lock lock : locks) {
				if (lock.type == TransactionManager.LockType.EXCLUSIVE)
					discardPage(lock.pid);
			}
		}
		
		transactionManager.release(tid);
	}
	
	/**
	 * Add a tuple to the specified table on behalf of transaction tid.  Will
	 * acquire a write lock on the page the tuple is added to and any other
	 * pages that are updated (Lock acquisition is not needed for lab2).
	 * May block if the lock(s) cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid     the transaction adding the tuple
	 * @param tableId the table to add the tuple to
	 * @param t       the tuple to add
	 */
	public void insertTuple(TransactionId tid, int tableId, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		
		DbFile file = Database.getCatalog().getDatabaseFile(tableId);
		ArrayList<Page> dirtyPages = file.insertTuple(tid, t);
		
		for (Page p : dirtyPages) {
			pin2pool(p);
			p.markDirty(true, tid);
		}
	}
	
	/**
	 * Remove the specified tuple from the buffer pool.
	 * Will acquire a write lock on the page the tuple is removed from and any
	 * other pages that are updated. May block if the lock(s) cannot be acquired.
	 * <p>
	 * Marks any pages that were dirtied by the operation as dirty by calling
	 * their markDirty bit, and adds versions of any pages that have
	 * been dirtied to the cache (replacing any existing versions of those pages) so
	 * that future requests see up-to-date pages.
	 *
	 * @param tid the transaction deleting the tuple.
	 * @param t   the tuple to delete
	 */
	public void deleteTuple(TransactionId tid, Tuple t)
			throws DbException, IOException, TransactionAbortedException {
		// some code goes here
		// not necessary for lab1
		
		DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
		ArrayList<Page> dirtyPages = file.deleteTuple(tid, t);
		
		for (Page p : dirtyPages) {
			pin2pool(p);
			p.markDirty(true, tid);
		}
	}
	
	/**
	 * Flush all dirty pages to disk.
	 * NB: Be careful using this routine -- it writes dirty data to disk so will
	 * break simpledb if running in NO STEAL mode.
	 */
	public synchronized void flushAllPages() throws IOException {
		// some code goes here
		// not necessary for lab1
		for (PageId pid : pid2page.keySet())
			flushPage(pid);
	}
	
	/**
	 * Remove the specific page id from the buffer pool.
	 * Needed by the recovery manager to ensure that the
	 * buffer pool doesn't keep a rolled back page in its
	 * cache.
	 * <p>
	 * Also used by B+ tree files to ensure that deleted pages
	 * are removed from the cache so they can be reused safely
	 */
	public synchronized void discardPage(PageId pid) {
		// some code goes here
		// not necessary for lab1
		pid2page.remove(pid);
	}
	
	/**
	 * Flushes a certain page to disk
	 *
	 * @param pid an ID indicating the page to flush
	 */
	private synchronized void flushPage(PageId pid) throws IOException {
		// some code goes here
		// not necessary for lab1
		// if present in buffer
		Page page = pid2page.get(pid);
		if (page.isDirty() != null) {
			Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
			page.markDirty(false, null);
		}
	}
	
	/**
	 * Write all pages of the specified transaction to disk.
	 */
	public synchronized void flushPages(TransactionId tid) throws IOException {
		// some code goes here
		// not necessary for lab1|lab2
		List<TransactionManager.Lock> locks = transactionManager.getLocksFromTid(tid);
		
		for (TransactionManager.Lock l : locks)
			if (l.type == TransactionManager.LockType.EXCLUSIVE && pid2page.containsKey(l.pid))
				flushPage(l.pid);
	}
	
	/**
	 * Discards a page from the buffer pool.
	 * Flushes the page to disk to ensure dirty pages are updated on disk.
	 */
	private synchronized void evictPage() throws DbException {
		// some code goes here
		// not necessary for lab1
		List<PageId> cleanPids = new LinkedList<>();
		for (PageId ePid : pid2page.keySet())
			if (pid2page.get(ePid).isDirty() == null)
				cleanPids.add(ePid);
		
		PageId evictPid = null;
		if (!cleanPids.isEmpty())
			evictPid = cleanPids.get(new Random().nextInt(cleanPids.size()));
		
		if (evictPid == null)
			throw new DbException("No page is clean, can not evict.");
		try {
			flushPage(evictPid);
			discardPage(evictPid);
		} catch (IOException e) {
			throw new DbException(e.getMessage());
		}
	}
}
