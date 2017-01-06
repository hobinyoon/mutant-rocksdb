package org.apache.cassandra.mutant;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemSsTableAccessMon
{
    // MemSsTableAccessMon keeps access statistics to Memtable and SSTables,
    // and periodically logs statistics. SstTempMon uses the SSTable access
    // statistics to make SSTable migration decisions. Each has it's own loop.
    //
    // SSTable access monitoring is for the SSTable migration decisions.
    // MemTable access monitoring is just to see/understand what's going on
    // internally.
    private static Map<Memtable, _MemTableAccCnt> _memTableAccCnt = new ConcurrentHashMap();
    private static Map<Descriptor, _SSTableAccCnt> _ssTableAccCnt = new ConcurrentHashMap();

    // These are for the initial get-and-set synchronizations.
    private static Object _memTableAccCntLock = new Object();
    private static Object _ssTableAccCntLock = new Object();

    private static volatile boolean _updatedSinceLastOutput = false;
    private static OutputRunnable _or = null;
    private static Thread _outThread = null;
    private static final Logger logger = LoggerFactory.getLogger(MemSsTableAccessMon.class);

    private static class _MemTableAccCnt {
        // We only keep the number of accesses here. We don't need to know if
        // the requested record is there or not.
        private AtomicLong accesses;
        private boolean discarded = false;
        private boolean loggedAfterDiscarded = false;

        public _MemTableAccCnt(long accesses) {
            this.accesses = new AtomicLong(accesses);
        }

        public void Increment() {
            this.accesses.incrementAndGet();
        }

        @Override
        public String toString() {
            return Long.toString(accesses.get());
            //StringBuilder sb = new StringBuilder(30);
            //sb.append(accesses.get());
            //return sb.toString();
        }
    }

    private static class _SSTableAccCnt {
        private SSTableReader _sstr;
        //private AtomicLong _bf_positives;

        // This is the actual number of accesses to the data file.
        // SSTableReader.getReadMeter().count() includes BF negatives.  You
        // still don't know if it hits the physical disk or not. Frequently
        // accessed blocks are cached in the Linux page cache.
        private AtomicLong _numNeedToReadDatafile;

        private boolean deleted = false;
        private boolean loggedAfterDiscarded = false;

        public _SSTableAccCnt(SSTableReader sstr, long numNeedToReadDataFile) {
            _sstr = sstr;
            //_bf_positives = new AtomicLong(0);
            _numNeedToReadDatafile = new AtomicLong(numNeedToReadDataFile);
        }

        //public void IncrementBfPositives() {
        //    _bf_positives.incrementAndGet();
        //}

        public void IncrementNumNeedToReadDataFile() {
            _numNeedToReadDatafile.incrementAndGet();
        }

        public long numNeedToReadDataFile() {
            return _numNeedToReadDatafile.get();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder(40);
            // Note: Keep the level here for now. It may change at runtime.
            sb.append(_sstr.getSSTableLevel())
                .append(",")
                .append(_numNeedToReadDatafile.get());
            return sb.toString();
            // We don't need bloom filter statistics for now.
            //StringBuilder sb = new StringBuilder(30);
            //sb.append(_numNeedToReadDatafile.get());
            //    // We don't need bloom filter statistics for now.
            //    //.append(",")._sstr.getReadMeter().count())
            //    ////.append(",").append(_bf_positives.get())
            //    //.append(",").append(_sstr.getBloomFilterTruePositiveCount())
            //    //.append(",").append(_sstr.getBloomFilterFalsePositiveCount())
        }
    }

    static {
        _or = new OutputRunnable();
        _outThread = new Thread(_or);
        _outThread.setName("MemSsTAccMon");
        _outThread.start();
        // Not sure where Cassandra handles SIGINT or SIGTERM, where I can
        // join() and clean up _outThread. It might be a crash only software
        // design.
    }

    // Called when a ColumnFamilyStore (table) is created.
    public static void Reset() {
        _memTableAccCnt.clear();
        _ssTableAccCnt.clear();
        logger.warn("Mutant: ResetMon");
        logger.warn("Mutant: Node configuration:[{}]", Config.GetNodeConfigStr());
    }

    public static void Update(Memtable m) {
        _updatedSinceLastOutput = true;

        // I wonder if you can get the min and max timestamps of records in the
        // memtable? I don't see them from Memtable.
        // - I can't remember why you needed it.
        _MemTableAccCnt v = _memTableAccCnt.get(m);
        if (v == null) {
            synchronized (_memTableAccCntLock) {
                v = _memTableAccCnt.get(m);
                if (v == null) {
                    _memTableAccCnt.put(m, new _MemTableAccCnt(1));
                    _or.Wakeup();
                } else {
                    v.Increment();
                }
            }
        } else {
            v.Increment();
        }
    }

    // These can be rewritten with test and test-and-set if needed.
    //
    //public static void Update(SSTableReader r) {
    //    Descriptor sst_desc = r.descriptor;
    //
    //    // The race condition (time of check and modify) that may overwrite the
    //    // first put() is harmless. It avoids an expensive locking.
    //    // Log right after the first access to a tablet, i.e., right after the
    //    // creation of _SSTableAccCnt(). It will help visualize the gap between
    //    // the creation of the tmp tablet and the first access to the regular
    //    // tablet.
    //    if (_ssTableAccCnt.get(sst_desc) == null) {
    //        _ssTableAccCnt.put(sst_desc, new _SSTableAccCnt(r));
    //        _updatedSinceLastOutput = true;
    //        _or.Wakeup();
    //    } else {
    //        _updatedSinceLastOutput = true;
    //    }
    //}
    //
    //public static void BloomfilterPositive(SSTableReader r) {
    //    Descriptor sst_desc = r.descriptor;
    //
    //    _SSTableAccCnt sstAC = _ssTableAccCnt.get(sst_desc);
    //    if (sstAC == null) {
    //        sstAC = new _SSTableAccCnt(r);
    //        sstAC.IncrementBfPositives();
    //        _ssTableAccCnt.put(sst_desc, sstAC);
    //        _updatedSinceLastOutput = true;
    //        _or.Wakeup();
    //    } else {
    //        sstAC.IncrementBfPositives();
    //        _updatedSinceLastOutput = true;
    //    }
    //}

    // Mutant: I wonder how much overhead does this Monitoring has.  Might be
    // a good one to present.

    public static void IncrementSstNeedToReadDataFile(SSTableReader r) {
        _updatedSinceLastOutput = true;
        Descriptor sst_desc = r.descriptor;
        _SSTableAccCnt sstAC = _ssTableAccCnt.get(sst_desc);

        // Test and test-and-set to make the synchronization overhead
        // minimal. It happens only in the beginning when _ssTableAccCnt
        // doesn't have the _SSTableAccCnt object.
        if (sstAC == null) {
            synchronized (_ssTableAccCntLock) {
                sstAC = _ssTableAccCnt.get(sst_desc);
                if (sstAC == null) {
                    _ssTableAccCnt.put(sst_desc, new _SSTableAccCnt(r, 1));
                    _or.Wakeup();
                } else {
                    sstAC.IncrementNumNeedToReadDataFile();
                }
            }
        } else {
            sstAC.IncrementNumNeedToReadDataFile();
        }
    }


    public static long GetNumSstNeedToReadDataFile(SSTableReader r) {
        _SSTableAccCnt sstAC = _ssTableAccCnt.get(r.descriptor);
        if (sstAC == null) {
            // TODO: what was this?
            // Harmless
            return 0;
        } else {
            return sstAC.numNeedToReadDataFile();
        }
    }


    // MemTable created
    public static void Created(Memtable m) {
        logger.warn("Mutant: MemtCreated {}", m);
        if (_memTableAccCnt.get(m) == null)
            _memTableAccCnt.put(m, new _MemTableAccCnt(0));
        _or.Wakeup();
    }

    // MemTable discarded
    public static void Discarded(Memtable m) {
        _MemTableAccCnt v = _memTableAccCnt.get(m);
        if (v == null) {
            // Can a memtable be discarded without being accessed at all? I'm
            // not sure, but let's not throw an exception.
            return;
        }
        v.discarded = true;

        _updatedSinceLastOutput = true;
        logger.warn("Mutant: MemtDiscard {}", m);
        _or.Wakeup();
    }

    private static SimpleDateFormat _sdf = new SimpleDateFormat("yyMMdd-HHmmss.SSS");

    public static void SstOpened(SSTableReader r) {
        Timestamp min_ts = new Timestamp(r.getMinTimestamp() / 1000);
        Timestamp max_ts = new Timestamp(r.getMaxTimestamp() / 1000);
        logger.warn("Mutant: SstOpened descriptor={} openReason={} bytesOnDisk()={}"
                + " level={} minTimestamp={} maxTimestamp={} first.getToken()={} last.getToken()={}"
                , r.descriptor, r.openReason, r.bytesOnDisk()
                , r.getSSTableLevel()
                , _sdf.format(min_ts), _sdf.format(max_ts)
                , r.first.getToken(), r.last.getToken()
                );
    }

    public static void SstCreated(SSTableReader r) {
        Timestamp min_ts = new Timestamp(r.getMinTimestamp() / 1000);
        Timestamp max_ts = new Timestamp(r.getMaxTimestamp() / 1000);
        logger.warn("Mutant: SstCreated descriptor={} openReason={} bytesOnDisk()={}"
                + " level={} minTimestamp={} maxTimestamp={} first.getToken()={} last.getToken()={}"
                , r.descriptor, r.openReason, r.bytesOnDisk()
                , r.getSSTableLevel()
                , _sdf.format(min_ts), _sdf.format(max_ts)
                , r.first.getToken(), r.last.getToken()
                );
    }

    // SSTable discarded
    public static void Deleted(Descriptor d) {
        _SSTableAccCnt v = _ssTableAccCnt.get(d);
        if (v == null) {
            // A SSTable can be deleted without having been accessed by
            // starting Cassandra, dropping an existing keyspace.
            return;
        }
        v.deleted = true;

        _updatedSinceLastOutput = true;
        logger.warn("Mutant: SstDeleted {}", d);
        _or.Wakeup();
    }

    private static class OutputRunnable implements Runnable {
        static final long reportIntervalMs =
            DatabaseDescriptor.getMutantOptions().tablet_access_stat_report_interval_in_ms;

        private final Object _sleepLock = new Object();

        void Wakeup() {
            synchronized (_sleepLock) {
                _sleepLock.notify();
            }
        }

        public void run() {
            // Sort lexicographcally with Memtables go first
            class OutputComparator implements Comparator<String> {
                @Override
                public int compare(String s1, String s2) {
                    if (s1.startsWith("Memtable-")) {
                        if (s2.startsWith("Memtable-")) {
                            return s1.compareTo(s2);
                        } else {
                            return -1;
                        }
                    } else {
                        if (s2.startsWith("Memtable-")) {
                            return 1;
                        } else {
                            return s1.compareTo(s2);
                        }
                    }
                }
            }
            OutputComparator oc = new OutputComparator();

            while (true) {
                synchronized (_sleepLock) {
                    try {
                        _sleepLock.wait(reportIntervalMs);
                    } catch(InterruptedException e) {
                        // It can wake up early to process Memtable /
                        // SSTable deletion events
                    }
                }

                // A non-strict but low-overhead serialization
                if (! _updatedSinceLastOutput)
                    continue;
                _updatedSinceLastOutput = false;

                // Remove discarded MemTables and SSTables after logging for the last time
                for (Iterator it = _memTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry pair = (Map.Entry) it.next();
                    _MemTableAccCnt v = (_MemTableAccCnt) pair.getValue();
                    if (v.discarded)
                        v.loggedAfterDiscarded = true;
                }
                // Remove deleted SSTables in the same way
                for (Iterator it = _ssTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry pair = (Map.Entry) it.next();
                    _SSTableAccCnt v = (_SSTableAccCnt) pair.getValue();
                    if (v.deleted)
                        v.loggedAfterDiscarded = true;
                }

                List<String> outEntries = new ArrayList();
                for (Iterator it = _memTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry pair = (Map.Entry) it.next();
                    Memtable m = (Memtable) pair.getKey();
                    outEntries.add(String.format("%s-%s"
                                , m.toString()
                                , pair.getValue().toString()));
                }

                // Note: Could reduce the log by printing out the diff.
                // SSTables without changes in counts are not printed.
                for (Iterator it = _ssTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry pair = (Map.Entry) it.next();
                    Descriptor d = (Descriptor) pair.getKey();
                    outEntries.add(String.format("%02d:%s"
                                //, d.cfname.substring(0, 2)
                                , d.generation
                                , pair.getValue().toString()));
                }

                // Remove Memtables and SSTables that are discarded and written to logs
                for (Iterator it = _memTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry pair = (Map.Entry) it.next();
                    _MemTableAccCnt v = (_MemTableAccCnt) pair.getValue();
                    if (v.loggedAfterDiscarded)
                        it.remove();
                }
                for (Iterator it = _ssTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
                    Map.Entry pair = (Map.Entry) it.next();
                    _SSTableAccCnt v = (_SSTableAccCnt) pair.getValue();
                    if (v.loggedAfterDiscarded)
                        it.remove();
                }

                if (outEntries.size() == 0)
                    continue;

                Collections.sort(outEntries, oc);

                StringBuilder sb = new StringBuilder(1000);
                boolean first = true;
                for (String i: outEntries) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(" ");
                    }
                    sb.append(i);
                }

                logger.warn("Mutant: TabletAccessStat {}", sb.toString());
            }
        }
    }
}
