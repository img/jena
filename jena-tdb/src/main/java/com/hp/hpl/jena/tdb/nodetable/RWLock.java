package com.hp.hpl.jena.tdb.nodetable;

import java.util.concurrent.locks.ReadWriteLock;

public class RWLock implements AutoCloseable {
	public enum Mode { READ, WRITE };
	final private ReadWriteLock l;
	final private Mode mode;

	public void close() throws Exception {
		if (mode.equals(Mode.READ)) {
			l.readLock().unlock();
		} else {
			l.writeLock().unlock();			
		}		
	}

	private RWLock(ReadWriteLock l, Mode mode) {
		this.l = l;
		this.mode = mode;
		if (mode.equals(Mode.READ)) {
			l.readLock().lock();
		} else {
			l.writeLock().lock();
		}
	}

	public static RWLock create(ReadWriteLock lock, Mode mode) {
		return new RWLock(lock, mode);
	}	
}
