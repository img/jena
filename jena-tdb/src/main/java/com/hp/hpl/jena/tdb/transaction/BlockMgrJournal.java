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
import java.util.HashSet ;
import java.util.Iterator ;
import java.util.Map ;
import java.util.Map.Entry;
import java.util.Set ;
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
    final private Map<Long, Block> freedBlocks = new HashMap<Long, Block>() ;
    private boolean closed  = false ;
    private boolean active  = false ;   // In a transaction, or preparing.
	private BlockMgr firstNonBMJ;
	private ConcurrentMap<Long, Block> aggregateWriteBlocks;
	
    public BlockMgrJournal(Transaction txn, FileRef fileRef, BlockMgr underlyingBlockMgr)
    {
        reset(txn, fileRef, underlyingBlockMgr) ;
        if ( txn.getMode() == ReadWrite.READ &&  underlyingBlockMgr instanceof BlockMgrJournal )
            System.err.println("Two level BlockMgrJournal") ;
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
    }

    @Override
    public void commitEnact(Transaction txn)
    {
        // No-op : this is done by playing the master journal.
    }

    @Override
    public void abort(Transaction txn)
    {
        checkActive() ;
        this.active = false ;
        // Do clearup of in-memory structures in clearup().
    }
    
    @Override
    public void commitClearup(Transaction txn)
    {
        // Persistent state is in the system journal.
        clear(txn) ;
    }
    
    /** Set, or reset, this BlockMgr.
     */
    private void reset(Transaction txn, FileRef fileRef, BlockMgr underlyingBlockMgr)
    {
        this.fileRef = fileRef ;
        this.blockMgr = underlyingBlockMgr ;
        this.active = true ;
        this.firstNonBMJ = getFirstNonBlockMgrJournal();
        if (underlyingBlockMgr instanceof BlockMgrJournal) {
        	// share with below
        	this.aggregateWriteBlocks = ((BlockMgrJournal)underlyingBlockMgr).aggregateWriteBlocks;        	
        	// update with that delta
        	for (Entry<Long, Block> e : ((BlockMgrJournal)underlyingBlockMgr).writeBlocks.entrySet()) {
        		this.aggregateWriteBlocks.put(e.getKey(), e.getValue());
        	}
        } else {
        	this.aggregateWriteBlocks = new ConcurrentHashMap<>();
        }
        clear(txn) ;
    }
    
    private BlockMgr getFirstNonBlockMgrJournal() {
    	BlockMgr bmj = blockMgr;
    	while (bmj instanceof BlockMgrJournal) {
    		if (bmj.isClosed()) {
    			System.err.println("img: wasnt expecting a closed BMJ here");
    		}
    		bmj = ((BlockMgrJournal)bmj).blockMgr;
    	}
    	return bmj;
	}
    

	private void clear(Transaction txn)
    {
        this.transaction = txn ;
        this.writeBlocks.clear() ;
        this.freedBlocks.clear() ;
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
    
    public Block getRead_cached(long id)
    {
        checkIfClosed() ;
        Block block = localBlock(id) ;
        if ( block != null )
            return block ;
        block = blockMgr.getRead(id) ;
        return block ;
    }
    
    
    private Block getRead_slow(long id)
    {
        checkIfClosed() ;
        Block block = localBlock(id) ;
        if ( block != null )
            return block ;
        block = aggregateWriteBlocks.get(id);
        if (null != block) {
        	return block;
        } else {
        	return firstNonBMJ.getRead(id);
        }
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
        	return block;
        } else {
        	block = firstNonBMJ.getReadIterator(id);
        	if (null == block) {
        		throw new BlockException("No such block: "+getLabel()+" "+id) ;
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
        	return;
        } else {
            firstNonBMJ.release(block) ;
        }
    }

    @Override
    public void write(Block block)
    {
        checkIfClosed() ;
        if ( ! block.isModified() )
            Log.warn(this, "Page for block "+fileRef+"/"+block.getId()+" not modified") ;
        
        if ( ! writeBlocks.containsKey(block.getId()) )
        {
            Log.warn(this, "Block not recognized: "+block.getId()) ;
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
        freedBlocks.put(block.getId(), block) ;
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
        	return true;
        } else {
        	// check the validator
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
            Log.fatal(this, "Already closed: "+transaction.getTxnId()) ;
    }

    private void checkActive()
    {
        if ( ! active )
            Log.fatal(this, "Not active: "+transaction.getTxnId()) ;
        TxnState state = transaction.getState() ; 
        if ( state != TxnState.ACTIVE && state != TxnState.PREPARING )
            Log.fatal(this, "**** Not active: "+transaction.getTxnId()) ;
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
        Log.info(this, "state: "+getLabel()) ;
        Log.info(this, "  writeBlocks:     "+writeBlocks) ;
        Log.info(this, "  freedBlocks:     "+freedBlocks) ;
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
    public String toString() { return "Journal:"+fileRef.getFilename()+" ("+blockMgr.getClass().getSimpleName()+")" ; }

    @Override
    public String getLabel() { return fileRef.getFilename() ; }
}
