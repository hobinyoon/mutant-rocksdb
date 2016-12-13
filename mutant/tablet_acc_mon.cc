#include <boost/format.hpp>

#include "mutant/tablet_acc_mon.h"
#include "util/util.h"

using namespace std;

namespace rocksdb {

class _MemtAccCnt {
  // The number of accesses to the MemTable. The requested record may or may
  // not be there.
  std::atomic<long> cnt;
  bool discarded = false;
  bool loggedAfterDiscarded = false;

public:
  _MemtAccCnt(long cnt_)
    : cnt(cnt_)
  { }

  void Increment() {
    cnt ++;
  }
};


class _SstAccCnt {
  std::atomic<long> cnt;
  bool deleted = false;
  bool loggedAfterDiscarded = false;

public:
  _SstAccCnt(long cnt_)
    : cnt(cnt_)
  { }

  void Increment() {
    cnt ++;
  }
};


TabletAccMon& TabletAccMon::_GetInst() {
  static TabletAccMon i;
  return i;
}


TabletAccMon::TabletAccMon()
: _updatedSinceLastOutput(false)
{
  TRACE << "TabletAccMon created\n";
}


void TabletAccMon::_MemtRead(void* m) {
  _updatedSinceLastOutput = true;

  // Test and test-and-set style
  auto it = _memtAccCnt.find(m);
  if (it == _memtAccCnt.end()) {
    lock_guard<mutex> _(_memtAccCntLock);

    auto it2 = _memtAccCnt.find(m);
    if (it2 == _memtAccCnt.end()) {
      _memtAccCnt[m] = new _MemtAccCnt(1);
      // TODO
      //_or.Wakeup();
    } else {
      (it2->second)->Increment();
    }
  } else {
    (it->second)->Increment();
  }

  TRACE << boost::format("Mutant: Memtable %p accessed\n") % m;
}


// I wonder how much overhead this Monitoring has.  Might be a good one to
// present.
void TabletAccMon::_SstRead(uint64_t s) {
  _updatedSinceLastOutput = true;

  // Test and test-and-set style. The longest path is taken only in the
  // beginning when _sstAccCnt doesn't have s.
  auto it = _sstAccCnt.find(s);
  if (it == _sstAccCnt.end()) {
    lock_guard<mutex> _(_sstAccCntLock);

    auto it2 = _sstAccCnt.find(s);
    if (it2 == _sstAccCnt.end()) {
      _sstAccCnt[s] = new _SstAccCnt(1);
      // TODO
      //_or.Wakeup();
    } else {
      (it2->second)->Increment();
    }
  } else {
    (it->second)->Increment();
  }

  TRACE << boost::format("Mutant: Read SSTable %d\n") % s;
}


void TabletAccMon::MemtRead(void* m) {
  TabletAccMon& i = _GetInst();
  i._MemtRead(m);
}


void TabletAccMon::SstRead(uint64_t s) {
  TabletAccMon& i = _GetInst();
  i._SstRead(s);
}

}


// package org.apache.cassandra.mutant;
//
// import java.sql.Timestamp;
// import java.text.SimpleDateFormat;
// import java.util.ArrayList;
// import java.util.Collections;
// import java.util.Comparator;
// import java.util.Map;
// import java.util.concurrent.atomic.AtomicLong;
// import java.util.concurrent.ConcurrentHashMap;
// import java.util.Iterator;
// import java.util.List;
//
// import org.apache.cassandra.config.Config;
// import org.apache.cassandra.config.DatabaseDescriptor;
// import org.apache.cassandra.db.Memtable;
// import org.apache.cassandra.io.sstable.Descriptor;
// import org.apache.cassandra.io.sstable.format.SSTableReader;
//
// import org.slf4j.Logger;
// import org.slf4j.LoggerFactory;
//
// public class MemSsTableAccessMon
// {
//
//     // These are for the initial get-and-set synchronizations.
//     private static Object _ssTableAccCntLock = new Object();
//
//     private static OutputRunnable _or = null;
//     private static Thread _outThread = null;
//     private static final Logger logger = LoggerFactory.getLogger(MemSsTableAccessMon.class);
//
//     static {
//         _or = new OutputRunnable();
//         _outThread = new Thread(_or);
//         _outThread.setName("MemSsTAccMon");
//         _outThread.start();
//         // Not sure where Cassandra handles SIGINT or SIGTERM, where I can
//         // join() and clean up _outThread. It might be a crash only software
//         // design.
//     }
//
//     // Called when a ColumnFamilyStore (table) is created.
//     public static void Reset() {
//         _memtAccCnt.clear();
//         _ssTableAccCnt.clear();
//         logger.warn("Mutant: ResetMon");
//         logger.warn("Mutant: Node configuration:[{}]", Config.GetNodeConfigStr());
//     }
//
//     // These can be rewritten with test and test-and-set if needed.
//     //
//     //public static void Update(SSTableReader r) {
//     //    Descriptor sst_desc = r.descriptor;
//     //
//     //    // The race condition (time of check and modify) that may overwrite the
//     //    // first put() is harmless. It avoids an expensive locking.
//     //    // Log right after the first access to a tablet, i.e., right after the
//     //    // creation of _SSTableAccCnt(). It will help visualize the gap between
//     //    // the creation of the tmp tablet and the first access to the regular
//     //    // tablet.
//     //    if (_ssTableAccCnt.get(sst_desc) == null) {
//     //        _ssTableAccCnt.put(sst_desc, new _SSTableAccCnt(r));
//     //        _updatedSinceLastOutput = true;
//     //        _or.Wakeup();
//     //    } else {
//     //        _updatedSinceLastOutput = true;
//     //    }
//     //}
//     //
//     //public static void BloomfilterPositive(SSTableReader r) {
//     //    Descriptor sst_desc = r.descriptor;
//     //
//     //    _SSTableAccCnt sstAC = _ssTableAccCnt.get(sst_desc);
//     //    if (sstAC == null) {
//     //        sstAC = new _SSTableAccCnt(r);
//     //        sstAC.IncrementBfPositives();
//     //        _ssTableAccCnt.put(sst_desc, sstAC);
//     //        _updatedSinceLastOutput = true;
//     //        _or.Wakeup();
//     //    } else {
//     //        sstAC.IncrementBfPositives();
//     //        _updatedSinceLastOutput = true;
//     //    }
//     //}
//
//     public static long GetNumSstNeedToReadDataFile(SSTableReader r) {
//         _SSTableAccCnt sstAC = _ssTableAccCnt.get(r.descriptor);
//         if (sstAC == null) {
//             // TODO: what was this?
//             // Harmless
//             return 0;
//         } else {
//             return sstAC.numNeedToReadDataFile();
//         }
//     }
//
//
//     // MemTable created
//     public static void Created(Memtable m) {
//         logger.warn("Mutant: MemtCreated {}", m);
//         if (_memtAccCnt.get(m) == null)
//             _memtAccCnt.put(m, new _MemtAccCnt(0));
//         _or.Wakeup();
//     }
//
//     // MemTable discarded
//     public static void Discarded(Memtable m) {
//         _MemtAccCnt v = _memtAccCnt.get(m);
//         if (v == null) {
//             // Can a memtable be discarded without being accessed at all? I'm
//             // not sure, but let's not throw an exception.
//             return;
//         }
//         v.discarded = true;
//
//         _updatedSinceLastOutput = true;
//         logger.warn("Mutant: MemtDiscard {}", m);
//         _or.Wakeup();
//     }
//
//     private static SimpleDateFormat _sdf = new SimpleDateFormat("yyMMdd-HHmmss.SSS");
//
//     public static void SstOpened(SSTableReader r) {
//         Timestamp min_ts = new Timestamp(r.getMinTimestamp() / 1000);
//         Timestamp max_ts = new Timestamp(r.getMaxTimestamp() / 1000);
//         logger.warn("Mutant: SstOpened descriptor={} openReason={} bytesOnDisk()={}"
//                 + " level={} minTimestamp={} maxTimestamp={} first.getToken()={} last.getToken()={}"
//                 , r.descriptor, r.openReason, r.bytesOnDisk()
//                 , r.getSSTableLevel()
//                 , _sdf.format(min_ts), _sdf.format(max_ts)
//                 , r.first.getToken(), r.last.getToken()
//                 );
//     }
//
//     public static void SstCreated(SSTableReader r) {
//         Timestamp min_ts = new Timestamp(r.getMinTimestamp() / 1000);
//         Timestamp max_ts = new Timestamp(r.getMaxTimestamp() / 1000);
//         logger.warn("Mutant: SstCreated descriptor={} openReason={} bytesOnDisk()={}"
//                 + " level={} minTimestamp={} maxTimestamp={} first.getToken()={} last.getToken()={}"
//                 , r.descriptor, r.openReason, r.bytesOnDisk()
//                 , r.getSSTableLevel()
//                 , _sdf.format(min_ts), _sdf.format(max_ts)
//                 , r.first.getToken(), r.last.getToken()
//                 );
//     }
//
//     // SSTable discarded
//     public static void Deleted(Descriptor d) {
//         _SSTableAccCnt v = _ssTableAccCnt.get(d);
//         if (v == null) {
//             // A SSTable can be deleted without having been accessed by
//             // starting Cassandra, dropping an existing keyspace.
//             return;
//         }
//         v.deleted = true;
//
//         _updatedSinceLastOutput = true;
//         logger.warn("Mutant: SstDeleted {}", d);
//         _or.Wakeup();
//     }
//
//     private static class OutputRunnable implements Runnable {
//         static final long reportIntervalMs =
//             DatabaseDescriptor.getMutantOptions().tablet_access_stat_report_interval_in_ms;
//
//         private final Object _sleepLock = new Object();
//
//         void Wakeup() {
//             synchronized (_sleepLock) {
//                 _sleepLock.notify();
//             }
//         }
//
//         public void run() {
//             // Sort lexicographcally with Memtables go first
//             class OutputComparator implements Comparator<String> {
//                 @Override
//                 public int compare(String s1, String s2) {
//                     if (s1.startsWith("Memtable-")) {
//                         if (s2.startsWith("Memtable-")) {
//                             return s1.compareTo(s2);
//                         } else {
//                             return -1;
//                         }
//                     } else {
//                         if (s2.startsWith("Memtable-")) {
//                             return 1;
//                         } else {
//                             return s1.compareTo(s2);
//                         }
//                     }
//                 }
//             }
//             OutputComparator oc = new OutputComparator();
//
//             while (true) {
//                 synchronized (_sleepLock) {
//                     try {
//                         _sleepLock.wait(reportIntervalMs);
//                     } catch(InterruptedException e) {
//                         // It can wake up early to process Memtable /
//                         // SSTable deletion events
//                     }
//                 }
//
//                 // A non-strict but low-overhead serialization
//                 if (! _updatedSinceLastOutput)
//                     continue;
//                 _updatedSinceLastOutput = false;
//
//                 // Remove discarded MemTables and SSTables after logging for the last time
//                 for (Iterator it = _memtAccCnt.entrySet().iterator(); it.hasNext(); ) {
//                     Map.Entry pair = (Map.Entry) it.next();
//                     _MemtAccCnt v = (_MemtAccCnt) pair.getValue();
//                     if (v.discarded)
//                         v.loggedAfterDiscarded = true;
//                 }
//                 // Remove deleted SSTables in the same way
//                 for (Iterator it = _ssTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
//                     Map.Entry pair = (Map.Entry) it.next();
//                     _SSTableAccCnt v = (_SSTableAccCnt) pair.getValue();
//                     if (v.deleted)
//                         v.loggedAfterDiscarded = true;
//                 }
//
//                 List<String> outEntries = new ArrayList();
//                 for (Iterator it = _memtAccCnt.entrySet().iterator(); it.hasNext(); ) {
//                     Map.Entry pair = (Map.Entry) it.next();
//                     Memtable m = (Memtable) pair.getKey();
//                     outEntries.add(String.format("%s-%s"
//                                 , m.toString()
//                                 , pair.getValue().toString()));
//                 }
//
//                 // Note: Could reduce the log by printing out the diff.
//                 // SSTables without changes in counts are not printed.
//                 for (Iterator it = _ssTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
//                     Map.Entry pair = (Map.Entry) it.next();
//                     Descriptor d = (Descriptor) pair.getKey();
//                     outEntries.add(String.format("%02d:%s"
//                                 //, d.cfname.substring(0, 2)
//                                 , d.generation
//                                 , pair.getValue().toString()));
//                 }
//
//                 // Remove Memtables and SSTables that are discarded and written to logs
//                 for (Iterator it = _memtAccCnt.entrySet().iterator(); it.hasNext(); ) {
//                     Map.Entry pair = (Map.Entry) it.next();
//                     _MemtAccCnt v = (_MemtAccCnt) pair.getValue();
//                     if (v.loggedAfterDiscarded)
//                         it.remove();
//                 }
//                 for (Iterator it = _ssTableAccCnt.entrySet().iterator(); it.hasNext(); ) {
//                     Map.Entry pair = (Map.Entry) it.next();
//                     _SSTableAccCnt v = (_SSTableAccCnt) pair.getValue();
//                     if (v.loggedAfterDiscarded)
//                         it.remove();
//                 }
//
//                 if (outEntries.size() == 0)
//                     continue;
//
//                 Collections.sort(outEntries, oc);
//
//                 StringBuilder sb = new StringBuilder(1000);
//                 boolean first = true;
//                 for (String i: outEntries) {
//                     if (first) {
//                         first = false;
//                     } else {
//                         sb.append(" ");
//                     }
//                     sb.append(i);
//                 }
//
//                 logger.warn("Mutant: TabletAccessStat {}", sb.toString());
//             }
//         }
//     }
// }
