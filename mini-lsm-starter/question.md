- what's the difference between a RWLock and a Mutex ? 
- add more doc to apply compaction task, what's the second returned paramter 
- fix book typo 
- why doesn't f64 implement the Ord trait so that it can be used in max_by_key 
- why flush and fsync in wal sync method ? 
- what is checksum ? what if checksum itself get corrupted ? 
- why do all the same keys with different timestamp need to stay in the same sstable? what breaks? 
- we iterate when inserting kvs into the memtable. what if a transaction sees a partially commited data ? one old key and a new key ( last question week 3 day 5 ) 
- maybe we can talk about SSI and SI in the book ? 

----------
- why hold tokio lock across await points ?
- what was the change in compaction thread ?
- why did we remove the lock observer from the manifest record and why was it needed in the first place ? 
- why was the lock_observer removed from force_freeze_memtable ?  
- what is aync-trait crate and how were the iterators made async ?
- is it wise to make iter async ? 
	- building blocks 
	- opening sstables 
	- creating and opening file object 
	- flushing memtables 
	- wal creation, syncing, and flushing 
	- manifest creation, syncing, and flushing 
	- iterator creation 

- why we removed Drop for mini-lsm struct ? 
- why file.into_std is used in risinglight

----------
async questions 
- what does the poll_fn() function do ? 

----------
TODO - async 
- prefetch next sstable in iter.next method 
- fix wal in async code 

----------
TODO - PRs 
- adding my solution 
- simplifying code in txn iterator 
- fixing book in tiered compaction 


