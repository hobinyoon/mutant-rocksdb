#include <atomic>
#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>

#include "db/event_helpers.h"
#include "db/version_edit.h"
#include "mutant/tablet_acc_mon.h"
#include "table/block_based_table_reader.h"
#include "util/util.h"

using namespace std;

namespace rocksdb {

// Note: make these configurable
const double _simulation_time_dur_sec =   60000.0  ;
const double _simulated_time_dur_sec  = 1365709.587;
const double _simulation_over_simulated_time_dur = _simulation_time_dur_sec / _simulated_time_dur_sec;
// 22.761826

const double TEMP_UNINITIALIZED = -1.0;
const double TEMP_DECAY_FACTOR = 0.99;


class SstMeta {
  boost::posix_time::ptime _created;
  uint64_t _size;
  long _initial_reads = 0;

  double _temp = TEMP_UNINITIALIZED;
  boost::posix_time::ptime _last_updated;

public:
  // This is called by _SstOpened() with a lock held.
  SstMeta(uint64_t size)
  : _created(boost::posix_time::microsec_clock::local_time())
    , _size(size)
  {
    //TRACE << boost::format("size=%d\n") % _size;
  }

  // These 2 are called with a lock held too. Plus, these are only called by
  // the reporter thread. Synchronization is not needed anyway.
  void UpdateTemp(long c, const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
      double age_simulated_time = (t - _created).total_nanoseconds() / 1000000000.0 / _simulation_over_simulated_time_dur;
      if (age_simulated_time < 30.0) {
        _initial_reads += c;
      } else {
        _initial_reads += c;
        _temp = _initial_reads / (_size / (64 * 1024.0 * 1024.0)) / age_simulated_time;
        _last_updated = t;
      }
    } else {
      _temp = _temp * pow(TEMP_DECAY_FACTOR, (t - _last_updated).total_nanoseconds() / 1000000000.0 / _simulation_over_simulated_time_dur)
        + c / (_size / (64 * 1024.0 * 1024.0)) * (1 - TEMP_DECAY_FACTOR);
      _last_updated = t;
    }
  }

  double Temp(const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
      // TODO: Handle undefined from the caller
      return _temp;
    } else {
      return _temp * pow(TEMP_DECAY_FACTOR, (t - _last_updated).total_nanoseconds() / 1000000000.0 / _simulation_over_simulated_time_dur);
    }
  }
};


TabletAccMon& TabletAccMon::_GetInst() {
  static TabletAccMon i;
  return i;
}


// This object is not released, since it's a singleton object.
TabletAccMon::TabletAccMon()
: _updatedSinceLastOutput(false)
  , _reporter_thread(thread(bind(&rocksdb::TabletAccMon::_ReporterRun, this)))
  , _smt_thread(thread(bind(&rocksdb::TabletAccMon::_SstMigrationTriggererRun, this)))
{
  // http://stackoverflow.com/questions/7381757/c-terminate-called-without-an-active-exception
  _reporter_thread.detach();
  _smt_thread.detach();
}


void TabletAccMon::_Init(EventLogger* logger) {
  _logger = logger;
  //TRACE << "TabletAccMon initialized\n";

  JSONWriter jwriter;
  EventHelpers::AppendCurrentTime(&jwriter);
  jwriter << "mutant_table_acc_mon_init";
  jwriter.EndObject();
  _logger->Log(jwriter);
}


void TabletAccMon::_MemtCreated(MemTable* m) {
  lock_guard<mutex> lk(_memtSetLock);

  //TRACE << boost::format("MemtCreated %p\n") % m;

  auto it = _memtSet.find(m);
  if (it == _memtSet.end()) {
    lock_guard<mutex> lk2(_memtSetLock2);
    _memtSet.insert(m);
    // Note: Log creation/deletion time, if needed. You don't need to wake up
    // the reporter thread.
  } else {
    THROW("Unexpected");
  }
}


void TabletAccMon::_MemtDeleted(MemTable* m) {
  lock_guard<mutex> lk(_memtSetLock);

  //TRACE << boost::format("MemtDeleted %p\n") % m;

  auto it = _memtSet.find(m);
  if (it == _memtSet.end()) {
    THROW("Unexpected");
  } else {
    // Report for the last time before erasing the entry
    _ReportAndWait();

    lock_guard<mutex> lk2(_memtSetLock2);
    _memtSet.erase(it);
  }
}


void TabletAccMon::_SstOpened(BlockBasedTable* bbt, uint64_t size) {
  lock_guard<mutex> lk(_sstMapLock);

  //TRACE << boost::format("SstOpened %d\n") % bbt->SstId();

  SstMeta* sm = new SstMeta(size);

  auto it = _sstMap.find(bbt);
  if (it == _sstMap.end()) {
    lock_guard<mutex> lk2(_sstMapLock2);
    _sstMap[bbt] = sm;
  } else {
    THROW("Unexpected");
  }
}


void TabletAccMon::_SstClosed(BlockBasedTable* bbt) {
  lock_guard<mutex> lk(_sstMapLock);

  //TRACE << boost::format("SstClosed %d\n") % bbt->SstId();

  auto it = _sstMap.find(bbt);
  if (it == _sstMap.end()) {
    THROW("Unexpected");
  } else {
    // Report for the last time before erasing the entry.
    _ReportAndWait();

    // You need a 2-level locking with _sstMapLock and _sstMapLock2.
    // _ReportAndWait() waits for the reporter thread to finish a cycle, which
    // acquires _sstMapLock2. The same goes with _memtSetLock2.
    lock_guard<mutex> lk2(_sstMapLock2);
    delete it->second;
    _sstMap.erase(it);
  }
}


void TabletAccMon::_Updated() {
  _updatedSinceLastOutput = true;
}


void TabletAccMon::_ReporterRun() {
  try {
    while (true) {
      _ReporterSleep();

      {
        lock_guard<mutex> lk(_reporting_mutex);

        // A 2-level check. _reporter_sleep_cv alone is not enough, since this
        // thread is woken up every second anyway.
        //
        // Print out the access stat. The entries are already sorted numerically.
        if (_updatedSinceLastOutput) {
          auto now = boost::posix_time::microsec_clock::local_time();

          // Output to the rocksdb log
          JSONWriter jwriter;
          EventHelpers::AppendCurrentTime(&jwriter);
          jwriter << "mutant_table_acc_cnt";
          {
            jwriter.StartObject();
            {
              vector<string> vs;
              {
                lock_guard<mutex> lk2(_memtSetLock2);
                for (auto mt: _memtSet) {
                  // Get-and-reset the read counter
                  long c = mt->_num_reads.exchange(0);
                  if (c > 0) {
                    // This doesn't work with JSONWriter. I guess only constant
                    // strings can be keys
                    //jwriter << mt.first << c;
                    vs.push_back(str(boost::format("%p:%d") % mt % c));
                  }
                }
              }
              if (vs.size() > 0)
                jwriter << "memt" << boost::algorithm::join(vs, " ");
            }
            {
              vector<string> vs;
              {
                lock_guard<mutex> lk2(_sstMapLock2);
                for (auto i: _sstMap) {
                  BlockBasedTable* bbt = i.first;
                  SstMeta* sm = i.second;
                  long c = bbt->GetAndResetNumReads();
                  if (c > 0) {
                    // Update temperature. You don't need to update sm when c != 0.
                    sm->UpdateTemp(c, now);

                    // TODO: The plotting script needs to be updated too
                    //vs.push_back(str(boost::format("%d:%d") % bbt->SstId() % c));
                    vs.push_back(str(boost::format("%d:%d:%.2f") % bbt->SstId() % c % sm->Temp(now)));
                  }
                }
              }
              if (vs.size() > 0)
                jwriter << "sst" << boost::algorithm::join(vs, " ");
            }
            jwriter.EndObject();
          }
          jwriter.EndObject();
          _logger->Log(jwriter);
          _updatedSinceLastOutput = false;
        }
        _reported = true;
      }
      _reported_cv.notify_one();
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


// Sleep for report_interval_simulated_time_ms or until woken up by another thread.
void TabletAccMon::_ReporterSleep() {
  static const long report_interval_simulated_time_ms = 30000;
  static const auto wait_dur = chrono::milliseconds(int(report_interval_simulated_time_ms * _simulation_over_simulated_time_dur));
  unique_lock<mutex> lk(_reporter_sleep_mutex);
  _reporter_sleep_cv.wait_for(lk, wait_dur, [&](){return _reporter_wakeupnow;});
  _reporter_wakeupnow = false;
}


void TabletAccMon::_ReporterWakeup() {
  // This wakes up the waiting thread even if this is called before wait.
  // _reporter_wakeupnow does the magic.
  {
    lock_guard<mutex> lk(_reporter_sleep_mutex);
    _reporter_wakeupnow = true;
  }
  _reporter_sleep_cv.notify_one();
}


void TabletAccMon::_ReportAndWait() {
  // The whole reporting block needs to be protected for this.  When the
  // reporter thread is ready entering the reporting block, the reporting block
  // can be called twice, which is okay - the second one won't print out
  // anything.
  {
    lock_guard<mutex> _(_reporting_mutex);
    _reported = false;
  }

  //TRACE << "force reporting\n";
  _ReporterWakeup();

  // Wait for the reporter finish a reporting
  {
    unique_lock<mutex> lk(_reporting_mutex);
    _reported_cv.wait(lk, [&](){return _reported;});
  }
}


// This thread triggers a SSTable compaction, which migrates SSTables, when the
// temperature of a SSTable drops below a threshold (*configurable*) and stays
// for 30 seconds (*configurable*).
void TabletAccMon::_SstMigrationTriggererRun() {
  return;

  try {
    while (true) {
      _SstMigrationTriggererSleep();

      {
        //lock_guard<mutex> lk(_smt_mutex);

        // TODO: Check if any SSTable has been cold
        // TODO: and trigger migration
        //static const long has_been_cold_for_threshold_simulated_time_ms = 30000;
        //static const long has_been_cold_for_threshold_ms = has_been_cold_for_threshold_simulated_time_ms * _simulation_over_simulated_time_dur;

        // TODO: Calc the temperature and when it drops below a threshold, e.g.
        // 1.0 (num reads with decay)/64MB/sec, mark the SSTable as cold, so
        // that the compaction manager can migrate it to a cold storage device.

        // TODO: THROW("Trigger migration");
      }
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}

void TabletAccMon::_SstMigrationTriggererSleep() {
  // Update every 1 minute in simulated time.
  // Note: Optionize later.
  static const long temp_update_interval_simulated_time_ms = 60000;
  static const long temp_update_interval_ms = temp_update_interval_simulated_time_ms * _simulation_over_simulated_time_dur;
  static const auto wait_dur = chrono::milliseconds(temp_update_interval_ms);
  unique_lock<mutex> lk(_smt_sleep_mutex);
  _smt_sleep_cv.wait_for(lk, wait_dur, [&](){return _smt_wakeupnow;});
  _smt_wakeupnow = false;
}


void TabletAccMon::_SstMigrationTriggererWakeup() {
  {
    lock_guard<mutex> lk(_smt_sleep_mutex);
    _smt_wakeupnow = true;
  }
  _smt_sleep_cv.notify_one();
}


void TabletAccMon::Init(EventLogger* logger) {
  static TabletAccMon& i = _GetInst();
  i._Init(logger);
}


void TabletAccMon::MemtCreated(MemTable* m) {
  static TabletAccMon& i = _GetInst();
  i._MemtCreated(m);
}


void TabletAccMon::MemtDeleted(MemTable* m) {
  static TabletAccMon& i = _GetInst();
  i._MemtDeleted(m);
}


// BlockBasedTable() calls this.
//
// FileDescriptor constructor/destructor was not good ones to monitor. They
// were copyable and the same address was opened and closed multiple times.
void TabletAccMon::SstOpened(BlockBasedTable* bbt, uint64_t size) {
  static TabletAccMon& i = _GetInst();
  i._SstOpened(bbt, size);
}


void TabletAccMon::SstClosed(BlockBasedTable* bbt) {
  static TabletAccMon& i = _GetInst();
  i._SstClosed(bbt);
}


void TabletAccMon::ReportAndWait() {
  static TabletAccMon& i = _GetInst();
  i._ReportAndWait();
}


void TabletAccMon::Updated() {
  static TabletAccMon& i = _GetInst();
  i._Updated();
}

}
