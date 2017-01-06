package org.apache.cassandra.mutant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.mutant.MemSsTableAccessMon;

// How are SstTempMon and MemSsTableAccessMon different?
// - MemSsTableAccessMon keeps access statistics to the MemTable and SSTables.
//   SstTempMon pulls the SSTable statistics periodically for making SSTable
//   migration decisions.
// - TODO: Do you want to refactor them?

public class SstTempMon {
    private static final Logger logger = LoggerFactory.getLogger(SstTempMon.class);

    private static final Map<SSTableReader, Monitor> monitors = new ConcurrentHashMap();

    private static final Set<SSTableReader> coldestSstrsReturned = new HashSet();

    // Called by DateTieredCompactionStrategy.addSSTable(SSTableReader
    // sstable). Seems like it is called at most once per sstable.
    public static void StartMonitor(ColumnFamilyStore cfs, SSTableReader sstr) {
        if ((! DatabaseDescriptor.getMutantOptions().migrate_sstables)
                || (! sstr.descriptor.mutantTable))
            return;

        // Only SSTables in hot storage are monitored
        if (sstr.StorageTemperatureLevel() > 0)
            return;

        if (monitors.containsKey(sstr)) {
            throw new RuntimeException(String.format("Unexpected: SSTableReader for %d is already in the map"
                        , sstr.descriptor.generation));
        }
        monitors.put(sstr, new Monitor(cfs, sstr));
    }

    // It is called multiple times for a sstable. It can also be called for a
    // sstable that doesn't have a temperature monitor started.  They are all
    // harmless.
    public static void StopMonitor(SSTableReader sstr) {
        if ((! DatabaseDescriptor.getMutantOptions().migrate_sstables)
                || (! sstr.descriptor.mutantTable))
            return;

        Monitor m = monitors.remove(sstr);
        if (m != null) {
            m.Stop();
        }

        synchronized (coldestSstrsReturned) {
            coldestSstrsReturned.remove(sstr);
        }
    }

    // Get a coldest candidate that is in the hot storage and colder than the
    // coldness threshold.
    //
    // I think returning a non-existant SSTableReader due to a race should be
    // okay. Will be taken care of by CompactionManager or something.
    //
    // Make sure a SStable is returned at most once. Otherwise, the second
    // CompactionExecutors pulls a duplicate one and runs a busy loop for quite
    // a while.
    public static SSTableReader GetColdest() {
        SSTableReader coldestSstr = null;
        long oldestBecameColdAtNs = 0;
        for (Iterator it = monitors.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry pair = (Map.Entry) it.next();
            SSTableReader r = (SSTableReader) pair.getKey();
            Monitor m = (Monitor) pair.getValue();
            if (r == null)
                continue;
            if (m.becameColdAt() == -1)
                continue;
            if (coldestSstr == null) {
                coldestSstr = r;
                oldestBecameColdAtNs = m.becameColdAt();
                continue;
            }
            if (m.becameColdAt() < oldestBecameColdAtNs) {
                coldestSstr = r;
                oldestBecameColdAtNs = m.becameColdAt();
            }
        }
        if (coldestSstr == null)
            return null;

        synchronized (coldestSstrsReturned) {
            if (coldestSstrsReturned.contains(coldestSstr))
                return null;
            coldestSstrsReturned.add(coldestSstr);
        }

        if (coldestSstr != null)
            logger.warn("Mutant: GetColdest()={}", coldestSstr.descriptor.generation);
        return coldestSstr;
    }

    private static class Monitor {
        private MonitorRunnable _mr;
        private Thread _thread;

        // Start a temperature monitor thread. The initial thread name is
        // attached to make sure that there is only one instance per
        // SSTableReader instance.
        Monitor(ColumnFamilyStore cfs, SSTableReader sstr) {
            _mr = new MonitorRunnable(cfs, sstr);
            _thread = new Thread(_mr);
            _thread.setName(String.format("SstTempMon-%d-%s",
                        sstr.descriptor.generation, _thread.getName()));
            _thread.start();
        }

        // Stop the temperature monitor thread and join
        public void Stop() {
            if (_mr != null)
                _mr.Stop();
            try {
                if (_thread != null) {
                    _thread.join();
                    _thread = null;
                }
            } catch (InterruptedException e) {
                logger.warn("Mutant: InterruptedException {}", e);
            }
            _mr = null;
        }

        public long becameColdAt() {
            return _mr.becameColdAt();
        }
    }

    private static class MonitorRunnable implements Runnable {
        private static final long sstMigrationDecisionIntervalMs;

        static {
            sstMigrationDecisionIntervalMs = DatabaseDescriptor.getMutantOptions().sst_migration_decision_interval_in_ms;
            logger.warn("Mutant: sstMigrationDecisionIntervalMs={}", sstMigrationDecisionIntervalMs);
        }

        private final ColumnFamilyStore cfs;

        // It is in fact when a tablet becomes available for reading
        private final long _tabletCreationTimeNs;
        private final SSTableReader _sstr;
        private final Object _sleepLock = new Object();
        private volatile boolean _stopRequested = false;
        private long becameColdAtNs = -1;

        public MonitorRunnable(ColumnFamilyStore cfs, SSTableReader sstr) {
            this.cfs = cfs;
            _tabletCreationTimeNs = System.nanoTime();
            _sstr = sstr;
        }

        public void Stop() {
            synchronized (_sleepLock) {
                //logger.warn("Mutant: gen={} stop requested", _sstr.descriptor.generation);
                _stopRequested = true;
                _sleepLock.notify();
            }
        }

        public long becameColdAt() {
            return becameColdAtNs;
        }

        // A sliding time window that monitors the number of
        // need-to-access-dfiles. This doesn't need to be multi-threaded.
        private static class AccessQueue {
            private static final double sstTempMonTimeWindowNs;

            static {
                sstTempMonTimeWindowNs = DatabaseDescriptor.getMutantOptions().sst_tempmon_time_window_in_sec
                    * 1000000000.0;
            }

            private class Element {
                // Simulation time
                private long timeNs;
                private long numAccesses;

                public Element(long timeNs, long numAccesses) {
                    this.timeNs = timeNs;
                    this.numAccesses = numAccesses;
                }
            }

            private long tabletCreationTimeNs;
            private Queue<Element> elements;
            private long numAccessesInQ;

            public AccessQueue(long tabletCreationTimeNs) {
                this.tabletCreationTimeNs = tabletCreationTimeNs;
                elements = new LinkedList();
                numAccessesInQ = 0;
            }

            public void DeqEnq(long curNs, long numAccesses) {
                // Delete expired elements
                while (true) {
                    Element e = elements.peek();
                    if (e == null)
                        break;
                    long accessTimeAgeNs = curNs - e.timeNs;
                    if (accessTimeAgeNs > sstTempMonTimeWindowNs) {
                        numAccessesInQ -= e.numAccesses;
                        elements.remove();
                    } else {
                        break;
                    }
                }

                // Add the new element
                if (numAccesses > 0) {
                    elements.add(new Element(curNs, numAccesses));
                    numAccessesInQ += numAccesses;
                }
            }

            // TODO: Below level n or TemperatureLevel() would be even better
            public boolean BelowColdnessThreshold(long curNano) {
                long monitorDurNs = curNano - tabletCreationTimeNs;
                if (monitorDurNs < sstTempMonTimeWindowNs) {
                    //logger.warn("Mutant: Not enough time has passed. monitorDurNs={}", monitorDurNs);
                    return false;
                }

                return (((double)numAccessesInQ) / sstTempMonTimeWindowNs
                        < DatabaseDescriptor.getMutantOptions().sst_tempmon_threshold_num_per_sec);
            }
        }

        public void run() {
            logger.warn("Mutant: TempMon Start {}", _sstr.descriptor.generation);
            long prevNnrd = -1;
            long prevNano = -1;
            AccessQueue q = new AccessQueue(_tabletCreationTimeNs);

            while (! _stopRequested) {
                synchronized (_sleepLock) {
                    try {
                        _sleepLock.wait(sstMigrationDecisionIntervalMs);
                    } catch(InterruptedException e) {
                    }
                }
                if (_stopRequested)
                    break;

                long curNano = System.nanoTime();
                // Number of need-to-read-datafiles
                long nnrd = MemSsTableAccessMon.GetNumSstNeedToReadDataFile(_sstr);

                if (prevNnrd == -1 || prevNano == -1) {
                    prevNnrd = nnrd;
                    prevNano = curNano;
                    continue;
                }

                q.DeqEnq(curNano, nnrd - prevNnrd);
                if (q.BelowColdnessThreshold(curNano)) {
                    becameColdAtNs = curNano;
                    logger.warn("Mutant: TempMon TabletBecomeCold {}", _sstr.descriptor.generation);
                    // TODO: Mark the sstable as cold. Or mark it as ready to
                    // be migrated to the next colder level.
                    //_sstr.BecomeCold();
                    CompactionManager.instance.submitBackground(cfs);
                    // Stop monitoring when a SSTable becomes cold.  SSTables
                    // only age. No anti-aging.
                    break;
                }

                prevNnrd = nnrd;
                prevNano = curNano;
            }

            logger.warn("Mutant: TempMon Stop {}", _sstr.descriptor.generation);
        }
    }
}
