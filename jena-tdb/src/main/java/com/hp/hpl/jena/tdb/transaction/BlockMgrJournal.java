/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hp.hpl.jena.tdb.transaction;

import java.util.HashMap ;
import java.util.Iterator ;
import java.util.Map ;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.openjena.atlas.logging.Log ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

import com.hp.hpl.jena.query.ReadWrite ;
import com.hp.hpl.jena.tdb.base.block.Block ;
import com.hp.hpl.jena.tdb.base.block.BlockException ;
import com.hp.hpl.jena.tdb.base.block.BlockMgr ;
import com.hp.hpl.jena.tdb.sys.FileRef ;

public class BlockMgrJournal implements BlockMgr, TransactionLifecycle
{
    private static Logger log = LoggerFactory.getLogger(BlockMgrJournal.class) ;
    private BlockMgr blockMgr ; // read-only except during journal checkpoint.
    private Transaction transaction ;
    private FileRef fileRef ;
    
    final private Map<Long, Block> writeBlocks = new HashMap<Long, Block>() ;
    private boolean closed  = false ;
    private boolean active  = false ;   // In a transaction, or preparing.
	private BlockMgr firstNonBMJ;
	private ConcurrentMap<Long, Block> aggregateWriteBlocks = new ConcurrentHashMap<>();
	
    public BlockMgrJournal(Transaction txn, FileRef fileRef, BlockMgr underlyingBlockMgr)
    {
    	log.debug("constructing BMJ with aggregating writeBlocks on " + fileRef.toString()); //$NON-NLS-1$
        reset(txn, fileRef, underlyingBlockMgr) ;
        if ( txn.getMode() == ReadWrite.READ &&  underlyingBlockMgr instanceof BlockMgrJournal )
            System.err.println("Two level BlockMgrJournal") ; //$NON-NLS-1$
    }
    
    

    @Override
    public void begin(Transaction txn)
    {
        reset(txn, fileRef, blockMgr) ;
    }
    
    @Override
    public void commitPrepare(Transaction txn)
    {
        checkActive() ;
        for ( Block blk : writeBlocks.values() )
            writeJournalEntry(blk) ;
        this.active = false ;
        log.debug("commitPrepare"); //$NON-NLS-1$
    }

    @Override
    public void commitEnact(Transaction txn)
    {
        // No-op : this is done by playing the master journal.
    	log.debug("commitEnact"); //$NON-NLS-1$
    }

    @Override
    public void abort(Transaction txn)
    {
        checkActive() ;
        this.active = false ;
        log.debug("abort"); //$NON-NLS-1$
        // Do clearup of in-memory structures in clearup().
    }
    
    @Override
    public void commitClearup(Transaction txn)
    {
        // Persistent state is in the system journal.
        clear(txn) ;
        log.debug("commitClearup"); //$NON-NLS-1$
    }
    
    /** Set, or reset, this BlockMgr.
     */
    private void reset(Transaction txn, FileRef fileRef, BlockMgr underlyingBlockMgr)
    {
        this.fileRef = fileRef ;
        this.blockMgr = underlyingBlockMgr ;
        this.active = true ;
        this.firstNonBMJ = getFirstNonBlockMgrJournal();
        clear(txn) ;
        long delta = 0;
        if (underlyingBlockMgr instanceof BlockMgrJournal) {
        	// copy from below
        	for (Entry<Long, Block> e : ((BlockMgrJournal)underlyingBlockMgr).aggregateWriteBlocks.entrySet()) {
        		this.aggregateWriteBlocks.put(e.getKey(), e.getValue());
        	}
        	delta = ((BlockMgrJournal)underlyingBlockMgr).writeBlocks.size();
        	// update with that delta. this CHM should be stable as the layers beneath are committed.
        	for (Entry<Long, Block> e : ((BlockMgrJournal)underlyingBlockMgr).writeBlocks.entrySet()) {
        		this.aggregateWriteBlocks.put(e.getKey(), e.getValue());
        	}
        }
        log.debug("aggregateWriteBlocks contains " + this.aggregateWriteBlocks.size() + " entries, of which " + delta + " are from previous layer"); //$NON-NLS-1$ //$NON-NLS-2$
        log.debug("reset"); //$NON-NLS-1$
    }
    
    private BlockMgr getFirstNonBlockMgrJournal() {
    	BlockMgr bmj = blockMgr;
    	while (bmj instanceof BlockMgrJournal) {
    		if (bmj.isClosed()) {
    			log.error("img: wasnt expecting a closed BMJ here"); //$NON-NLS-1$
    		}
    		bmj = ((BlockMgrJournal)bmj).blockMgr;
    	}
    	return bmj;
	}
    

	private void clear(Transaction txn)
    {
        this.transaction = txn ;
        this.writeBlocks.clear() ;
        log.debug("clear"); //$NON-NLS-1$
    }
                       
    @Override
    public Block allocate(int blockSize)
    {
        checkIfClosed() ;
        // Might as well allocate now. 
        // This allocates the id.
        Block block = firstNonBMJ.allocate(blockSize) ;
        // [TxTDB:TODO]
        // But we "copy" it by allocating ByteBuffer space.
        if ( active ) 
        {
            block = block.replicate( ) ;
            writeBlocks.put(block.getId(), block) ;
        }
        return block ;
    }

    @Override
    public Block getRead(long id)
    {
    	return getRead_cached(id);
    }
    
    public Block getRead_slow(long id)
    {
        checkIfClosed() ;
        Block block = localBlock(id) ;
        if ( block != null )
            return block ;
        block = blockMgr.getRead(id) ;
        return block ;
    }
    
    
    private Block getRead_cached(long id)
    {
        checkIfClosed() ;
        Block block = localBlock(id) ;
        if ( block != null )
            return block ;
        block = aggregateWriteBlocks.get(id);
        if (null != block) {
        	logHit();
        	return block;
        } else {
        	logMiss();
        	return firstNonBMJ.getRead(id);
        }
    }

    private void logMiss() {
		//log.debug("aggregate cache miss"); //$NON-NLS-1$
	}



	private void logHit() {
		//log.debug("aggregate cache hit"); //$NON-NLS-1$
	}



	@Override
    public Block getReadIterator(long id)
    {
        //logState() ;
        checkIfClosed() ;
        Block block = localBlock(id) ;
        if ( block == null ) {
        	block = aggregateWriteBlocks.get(id) ;
        }
        if (null != block) {
        	logHit();
        	return block;
        } else {
        	logMiss();
        	block = firstNonBMJ.getReadIterator(id);
        	if (null == block) {
        		throw new BlockException("No such block: "+getLabel()+" "+id) ; //$NON-NLS-1$ //$NON-NLS-2$
        	}
        	return block ;
        }
    }

    @Override
    public Block getWrite(long id)
    {
        // NB: If we are in a stack of BlockMgrs, after a transaction has committed,
        // we would be called via getRead and the upper Blockgr does the promotion. 
        checkActive() ;
        checkIfClosed() ;
        Block block = localBlock(id) ;
        if ( block != null )
            return block ;
        
        // Get-as-read.
        //block = blockMgr.getRead(id) ;
        block = getRead(id);  // repeats the localBlock code
        // If most blocks get modified, then a copy is needed
        // anyway so now is as good a time as any.
        block = _promote(block) ;
        return block ;
    }

    private Block localBlock(long id)
    {
        checkIfClosed() ;
        return writeBlocks.get(id) ;
    }
    
    @Override
    public Block promote(Block block)
    {
        checkIfClosed() ;
        if ( writeBlocks.containsKey(block.getId()) )
            return block ;
        return _promote(block) ;
    }

    private Block _promote(Block block)
    {
        checkActive() ; 
        block = block.replicate() ;
        writeBlocks.put(block.getId(), block) ;
        return block ;
    }

    @Override
    public void release(Block block)
    {
        checkIfClosed() ;
        Long id = block.getId() ;
        // Only release unchanged blocks.
        if (writeBlocks.containsKey(id)
        		|| aggregateWriteBlocks.containsKey(id)) {
        	logHit(); // not quite
        	return;
        } else {
        	logMiss();
            firstNonBMJ.release(block) ;
        }
    }

    @Override
    public void write(Block block)
    {
        checkIfClosed() ;
        if ( ! block.isModified() )
            Log.warn(this, "Page for block "+fileRef+"/"+block.getId()+" not modified") ; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        
        if ( ! writeBlocks.containsKey(block.getId()) )
        {
            Log.warn(this, "Block not recognized: "+block.getId()) ; //$NON-NLS-1$
            // Probably corruption by writing in-place.
            // but at least when this transaction commits,
            // the update data is written,
            writeBlocks.put(block.getId(), block) ;
        }
    }
    
    @Override
    public void overwrite(Block block)
    {
        // We are in a chain of BlockMgrs - pass down to the base.
    	firstNonBMJ.overwrite(block) ;
    }

    @Override
    public void free(Block block)
    {
        checkIfClosed() ;
    }

    @Override
    public boolean isEmpty()
    {
        checkIfClosed() ;
        return writeBlocks.isEmpty() && aggregateWriteBlocks.isEmpty() && firstNonBMJ.isEmpty();
    }

    @Override

    public boolean valid(int id)
    {
        return valid_cached(id);
    }
    
    private boolean valid_cached(int id)
    {
        checkIfClosed() ;
        if ( writeBlocks.containsKey(id) ) return true ;
        Block b = aggregateWriteBlocks.get(id);
        if (null != b) {
        	logHit();
        	return true;
        } else {
        	logMiss();
        	return firstNonBMJ.valid(id);
        }
    }
    
    private boolean valid_slow(int id)
    {
        checkIfClosed() ;
        if ( writeBlocks.containsKey(id) ) return true ;
        return blockMgr.valid(id) ; 
    }

    @Override
    public void close()
    {
        closed = true ;
    }

    @Override
    public boolean isClosed()
    {
        return closed ;
    }
    
    private void checkIfClosed()
    {
        if ( closed )
            Log.fatal(this, "Already closed: "+transaction.getTxnId()) ; //$NON-NLS-1$
    }

    private void checkActive()
    {
        if ( ! active )
            Log.fatal(this, "Not active: "+transaction.getTxnId()) ; //$NON-NLS-1$
        TxnState state = transaction.getState() ; 
        if ( state != TxnState.ACTIVE && state != TxnState.PREPARING )
            Log.fatal(this, "**** Not active: "+transaction.getTxnId()) ; //$NON-NLS-1$
    }


    @Override
    public void sync()
    {
        checkIfClosed() ;
    }
    
    @Override
    public void syncForce()
    {
        firstNonBMJ.syncForce() ;
    }

    // we only use the underlying blockMgr in read-mode - we don't write back blocks.  
    @Override
    public void beginUpdate() {
    	checkIfClosed() ; 
    	blockMgr.beginRead() ; 
    }

    @Override
    public void endUpdate()
    {
        checkIfClosed() ;
        blockMgr.endRead() ;
    }

    private void writeJournalEntry(Block blk)
    {
        blk.getByteBuffer().rewind() ;
        transaction.getJournal().write(JournalEntryType.Block, fileRef, blk) ;
    }
    
    private void logState()
    {
        Log.info(this, "state: "+getLabel()) ; //$NON-NLS-1$
        Log.info(this, "  writeBlocks:     "+writeBlocks) ; //$NON-NLS-1$
    }
    
    @Override
    public void beginRead() { 
    	checkIfClosed() ;
    	firstNonBMJ.beginRead() ;
    }

    @Override
    public void endRead() {
    	checkIfClosed() ; 
    	firstNonBMJ.endRead() ;
    }

    @Override
    public void beginIterator(Iterator<?> iterator)
    {
        checkIfClosed() ; 
        transaction.addIterator(iterator) ;
        // Don't pass down the beginIterator call - we track and manage here, not lower down.  
        //blockMgr.beginIterator(iterator) ;
    }

    @Override
    public void endIterator(Iterator<?> iterator)
    {
        checkIfClosed() ; 
        transaction.removeIterator(iterator) ;
        // Don't pass down the beginIterator call - we track and manage here, not lower down.  
        //blockMgr.endIterator(iterator) ;
    }

    @Override
    public String toString() { return "Journal:"+fileRef.getFilename()+" ("+blockMgr.getClass().getSimpleName()+")" ; } //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    @Override
    public String getLabel() { return fileRef.getFilename() ; }
}
