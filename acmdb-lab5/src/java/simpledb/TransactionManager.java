package simpledb;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionManager {
	private ConcurrentHashMap<TransactionId, List<Lock>> trans2lock;
	private ConcurrentHashMap<PageId, List<Lock>> page2write;
	private ConcurrentHashMap<PageId, List<Lock>> page2read;
	private Random rand = new Random(0);
	private Object synchronizer;
	
	public class Lock {
		public TransactionId tid;
		public PageId pid;
		public LockType type;
		
		Lock(TransactionId tid, PageId pid, LockType t) {
			this.tid = tid;
			this.pid = pid;
			type = t;
		}
	}
	
	public enum LockType {
		EXCLUSIVE, SHARED
	}
	
	public TransactionManager(Object synchronizer) {
		trans2lock = new ConcurrentHashMap<>();
		page2write = new ConcurrentHashMap<>();
		page2read = new ConcurrentHashMap<>();
		this.synchronizer = synchronizer;
	}
	
	public List<Lock> getLocksFromTid(TransactionId tid) {
		return trans2lock.getOrDefault(tid, new ArrayList<>());
	}
	
	private void nap_mayAbort(long startTime) throws TransactionAbortedException {
		int NAP_TIME = 50;
		int ABORT_RAND_RANGE = 400;
		int ABORT_BASE = 200;
		
		long curTime = System.currentTimeMillis();
		if (curTime - startTime > ABORT_BASE + rand.nextInt(ABORT_RAND_RANGE)) {
			throw new TransactionAbortedException();
		}
		
		try {
			synchronizer.wait(NAP_TIME);
		} catch (InterruptedException e) {
		}
	}
	
	
	public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
		if (!trans2lock.containsKey(tid))
			trans2lock.put(tid, new ArrayList<>());

		Lock lock;
		long start = System.currentTimeMillis();

		if (perm == Permissions.READ_ONLY) {
			if (!page2read.containsKey(pid))
				page2read.put(pid, new ArrayList<>());
			if (!page2write.containsKey(pid))
				page2write.put(pid, new ArrayList<>());

			while (!page2write.get(pid).isEmpty() && page2write.get(pid).get(0).tid != tid)
				nap_mayAbort(start);

			lock = new Lock(tid, pid, LockType.SHARED);
			page2read.get(pid).add(lock);

		} else {
			if (!page2write.containsKey(pid))
				page2write.put(pid, new ArrayList<>());
			if (!page2read.containsKey(pid))
				page2read.put(pid, new ArrayList<>());
//
			while (true) {
				boolean otherWrite = (!page2write.get(pid).isEmpty() && page2write.get(pid).get(0).tid != tid);
				boolean otherRead = false;
				for (Lock l : page2read.get(pid))
					if (l.tid != tid)
						otherRead = true;
				if (!otherWrite && !otherRead)
					break;

				nap_mayAbort(start);
			}


			lock = new Lock(tid, pid, LockType.EXCLUSIVE);
			page2write.get(pid).add(lock);
		}
		trans2lock.get(tid).add(lock);

	}
	
	public void release(TransactionId tid, PageId pid) {
		trans2lock.getOrDefault(tid, new LinkedList<>()).removeIf(lock -> lock.pid == pid);
		page2write.getOrDefault(pid, new LinkedList<>()).removeIf(lock -> lock.tid == tid);
		page2read.getOrDefault(pid, new LinkedList<>()).removeIf(lock -> lock.tid == tid);
		
		synchronizer.notifyAll();
	}
	
	public void release(TransactionId tid) {
		if (trans2lock.containsKey(tid)) {
			List<Lock> locks = trans2lock.get(tid);
			for (Lock l : locks) {
				PageId pageId = l.pid;
				page2write.get(pageId).removeIf(lock -> lock.tid == tid);
				page2read.get(pageId).removeIf(lock -> lock.tid == tid);
			}
			locks.clear();
			trans2lock.remove(tid);
		}
		
		synchronizer.notifyAll();
	}
	
	public boolean holdsLock(TransactionId tid, PageId pid) {
		for (Lock lock : trans2lock.get(tid))
			if (lock.pid == pid) return true;
		return false;
	}
	
}