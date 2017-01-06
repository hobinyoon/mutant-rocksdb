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
//const double _simulation_time_dur_sec =   60000.0  ;
// For fast dev. On mjolnir, without limiting memory:
//   With 6000 secs, about 90% CPU idle.
//   With 4000 secs, about 85% CPU idle.
//   With 2000 secs, about 55% CPU idle. I think this is good enough.

// Working on multi db_paths
const double _simulation_time_dur_sec =   1000.0  ;
const double _simulated_time_dur_sec  = 1365709.587;
const double _simulation_over_simulated_time_dur = _simulation_time_dur_sec / _simulated_time_dur_sec;
// 22.761826, with the 60,000 sec simulation time

const double TEMP_UNINITIALIZED = -1.0;
const double MIN_AGE_SEC_BEFORE_INIT_TEMP_SIMULATED_TIME = 600.0;

const double TEMP_DECAY_FACTOR = 0.999;

const double SST_TEMP_BECOME_COLD_THRESHOLD = 20.0;

// Let's not make it more complicated than necessary for now
//const double HAS_BEEN_COLD_FOR_THRESHOLD_IN_SEC = 30.0;

const long REPORT_INTERVAL_SEC_SIMULATED_TIME = 60;


class SstTemp {
  BlockBasedTable* _bbt;
  uint64_t _sst_id;
  boost::posix_time::ptime _created;
  uint64_t _size;
  int _level;
  long _initial_reads = 0;

  double _temp = TEMP_UNINITIALIZED;
  boost::posix_time::ptime _last_updated;

  // TODO: Let's not complicate it for now
  // May want to define 4 different levels.
  // For now just 2:
  //   0: cold
  //   1: hot
  //int _temp_level = 0;
  //boost::posix_time::ptime _became_cold_at_simulation_time;
  //bool _became_cold_at_defined = false;

public:
  // This is called by _SstOpened() with the locks (_sstMapLock, _sstMapLock2) held.
  // level is -1 for some of the SSTables.
  // - It might be because the pre-existing SSTables are opened. -1 is the
  //   default value of one of the functions. From the temperature, it seems
  //   like those with -1 can be at any level.  It doesn't matter for now.
  SstTemp(BlockBasedTable* bbt, uint64_t sst_id, uint64_t size, int level)
  : _bbt(bbt)
    , _sst_id(sst_id)
    , _created(boost::posix_time::microsec_clock::local_time())
    , _size(size)
    , _level(level)
  {
    //TRACE << boost::format("size=%d\n") % _size;
  }

  long GetAndResetNumReads() {
    return _bbt->GetAndResetNumReads();
  }

  // This is called with _sstMapLock2 held.
  void UpdateTemp(long reads, double reads_per_64MB_per_sec, const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
      double age_simulated_time = (t - _created).total_nanoseconds() / 1000000000.0 / _simulation_over_simulated_time_dur;
      if (age_simulated_time < MIN_AGE_SEC_BEFORE_INIT_TEMP_SIMULATED_TIME) {
        _initial_reads += reads;
      } else {
        // When there is a gap between the creation and the first hits, use
        // reads_per_64MB_per_sec. RocksDB might have been busy not having time
        // to update the stat.
        if (_initial_reads == 0) {
          _temp = reads_per_64MB_per_sec;
        } else {
          //  When there have been reads before (no gap), use the average
          //  during the time window.
          _initial_reads += reads;
          _temp = _initial_reads / (_size / (64.0*1024*1024)) / age_simulated_time;
        }

        // TODO: Setting temp_level can be separated from here. It can be done
        // in the triggerer thread.
        //
        // Here, _temp_level is always 0 here. When _temp is cold, assume the
        // SSTable has been cold from the beginning.
        //if (_temp < SST_TEMP_BECOME_COLD_THRESHOLD) {
        //  _became_cold_at_simulation_time = _created;
        //  _became_cold_at_defined = true;

        //  // Mark as cold
        //  _temp_level = 1;
        //  TRACE << boost::format("Sst %d:%d became cold at %s in simulation time\n")
        //    % _sst_id % _level % _became_cold_at_simulation_time;
        //  // TODO: trigger migration. either here or using a separate
        //  // thread. TODO: check out the compation code and figure out how
        //  // you do it.
        //}

        _last_updated = t;
      }
    } else {
      double dur_since_last_update_in_sec_simulated_time = (t - _last_updated).total_nanoseconds()
        / 1000000000.0 / _simulation_over_simulated_time_dur;
      // Treat reads happened in the middle of (_last_updated, t), thus the / 2.0.
      _temp = _temp * pow(TEMP_DECAY_FACTOR, dur_since_last_update_in_sec_simulated_time)
        + reads / (_size / (64.0*1024*1024))
        * pow(TEMP_DECAY_FACTOR, dur_since_last_update_in_sec_simulated_time / 2.0)
        * (1 - TEMP_DECAY_FACTOR);

      //if (_temp_level == 0) {
      //  // Note: update to "temperature level changed". Not sure if there is any
      //  // SSTable "becoming hot again".
      //  //
      //  // SSTable becoming colder
      //  if (_temp < SST_TEMP_BECOME_COLD_THRESHOLD) {
      //    if (_became_cold_at_defined) {
      //      double been_cold_for = (t - _became_cold_at_simulation_time).total_nanoseconds()
      //        / 1000000000.0 / _simulation_over_simulated_time_dur;
      //      if (HAS_BEEN_COLD_FOR_THRESHOLD_IN_SEC < been_cold_for) {
      //        // Mark as cold
      //        _temp_level = 1;
      //        TRACE << boost::format("Sst %d:%d became cold at %s in simulation time, been_cold_for %.2f secs in simulated time\n")
      //          % _sst_id % _level % _became_cold_at_simulation_time % been_cold_for;
      //        // TODO: trigger migration. either here or using a separate
      //        // thread. TODO: check out the compation code and figure out how
      //        // you do it.
      //      }
      //    } else {
      //      // SST_TEMP_BECOME_COLD_THRESHOLD * pow(TEMP_DECAY_FACTOR, been_cold_for) = _temp
      //      // pow(TEMP_DECAY_FACTOR, been_cold_for) = _temp / SST_TEMP_BECOME_COLD_THRESHOLD
      //      // been_cold_for = log_(TEMP_DECAY_FACTOR) (_temp / SST_TEMP_BECOME_COLD_THRESHOLD)
      //      //               = log(_temp / SST_TEMP_BECOME_COLD_THRESHOLD) / log(TEMP_DECAY_FACTOR)
      //      double been_cold_for = log(_temp / SST_TEMP_BECOME_COLD_THRESHOLD) / log(TEMP_DECAY_FACTOR);
      //      TRACE << boost::format("Sst %d:%d been_cold_for %.2f secs in simulated time\n")
      //        % _sst_id % _level % been_cold_for;
      //      if (dur_since_last_update_in_sec_simulated_time < been_cold_for)
      //        THROW("Unexpected");

      //      // Resolution independent fractional second
      //      // http://www.boost.org/doc/libs/1_63_0/doc/html/date_time/posix_time.html#date_time.posix_time.time_duration
      //      {
      //        long cnt = boost::posix_time::time_duration::ticks_per_second()
      //          * been_cold_for * _simulation_over_simulated_time_dur;
      //        _became_cold_at_simulation_time = t - boost::posix_time::time_duration(0, 0, 0, cnt);
      //      }
      //      _became_cold_at_defined = true;

      //      if (HAS_BEEN_COLD_FOR_THRESHOLD_IN_SEC < been_cold_for) {
      //        // Mark as cold
      //        _temp_level = 1;
      //        TRACE << boost::format("Sst %d:%d became cold at %s in simulation time, been_cold_for %.2f secs in simulated time\n")
      //          % _sst_id % _level % _became_cold_at_simulation_time % been_cold_for;
      //        // TODO: trigger migration. either here or using a separate
      //        // thread. TODO: check out the compation code and figure out how
      //        // you do it.
      //      }
      //    }
      //  }
      //}

      // TODO: SSTable "becoming hotter".  Not sure if there is any SSTable
      // that becomes hotter.  We assume the temperature changed at the time of
      // this function call.

      _last_updated = t;
    }
  }

  // This is called with _sstMapLock2 held.
  double Temp(const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
      // TODO: Handle undefined from the caller
      return _temp;
    } else {
      return _temp * pow(TEMP_DECAY_FACTOR, (t - _last_updated).total_nanoseconds()
          / 1000000000.0 / _simulation_over_simulated_time_dur);
    }
  }

  int Level() {
    return _level;
  }

  uint64_t Size() {
    return _size;
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


void TabletAccMon::_SstOpened(BlockBasedTable* bbt, uint64_t size, int level) {
  lock_guard<mutex> lk(_sstMapLock);
  //TRACE << boost::format("SstOpened %d\n") % bbt->SstId();

  uint64_t sst_id = bbt->SstId();
  SstTemp* st = new SstTemp(bbt, sst_id, size, level);

  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end()) {
    lock_guard<mutex> lk2(_sstMapLock2);
    _sstMap[sst_id] = st;
  } else {
    THROW("Unexpected");
  }
}


void TabletAccMon::_SstClosed(BlockBasedTable* bbt) {
  lock_guard<mutex> lk(_sstMapLock);
  uint64_t sst_id = bbt->SstId();
  //TRACE << boost::format("SstClosed %d\n") % sst_id;
  auto it = _sstMap.find(sst_id);
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


void TabletAccMon::_SetUpdated() {
  _updatedSinceLastOutput = true;
}


double TabletAccMon::_Temperature(uint64_t sst_id, const boost::posix_time::ptime& cur_time) {
  lock_guard<mutex> _(_sstMapLock2);
  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end())
    THROW(boost::format("Unexpected: sst_id=%d") % sst_id);
  SstTemp* st = it->second;
  return st->Temp(cur_time);
}


uint32_t TabletAccMon::_CalcOutputPathId(const std::vector<FileMetaData*>& file_metadata) {
  if (file_metadata.size() == 0)
    THROW("Unexpected");

  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

  vector<double> input_sst_temp;
  vector<double> input_sst_path_id;

  for (const auto& fmd: file_metadata) {
    uint64_t sst_id = fmd->fd.GetNumber();
    uint32_t path_id = fmd->fd.GetPathId();
    double temp = _Temperature(sst_id, cur_time);

    if (temp != -1.0)
      input_sst_temp.push_back(temp);

    input_sst_path_id.push_back(path_id);

    TRACE << boost::format("%d Input Sst: sst_id=%d path_id=%d temp=%.3f\n")
      % std::this_thread::get_id() % sst_id % path_id % temp;
  }

  // Output path_id starts from the min of input path_ids
  uint32_t output_path_id = *std::min_element(input_sst_path_id.begin(), input_sst_path_id.end());

  // If the average input SSTable tempereture is below a threshold, set the
  // output path_id accordingly.
  if (input_sst_temp.size() > 0) {
    double avg = std::accumulate(input_sst_temp.begin(), input_sst_temp.end(), 0.0) / input_sst_temp.size(); 
    if (avg < SST_TEMP_BECOME_COLD_THRESHOLD) {
      output_path_id = 1;
    }
  }
  TRACE << boost::format("%d Output Sst path_id=%d\n")
    % std::this_thread::get_id() % output_path_id;
  return output_path_id;
}


void TabletAccMon::_ReporterRun() {
  try {
    boost::posix_time::ptime prev_time;

    while (true) {
      _ReporterSleep();

      {
        lock_guard<mutex> lk(_reporting_mutex);

        boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

        // A 2-level check. _reporter_sleep_cv alone is not enough, since this
        // thread is woken up every second anyway.
        //
        // Print out the access stat. The entries are already sorted numerically.
        if (_updatedSinceLastOutput) {
          vector<string> memt_stat_str;
          {
            lock_guard<mutex> lk2(_memtSetLock2);
            for (auto mt: _memtSet) {
              // Get-and-reset the read counter
              long c = mt->_num_reads.exchange(0);
              if (c > 0) {
                // This doesn't work with JSONWriter. I guess only constant
                // strings can be keys
                //jwriter << mt.first << c;
                memt_stat_str.push_back(str(boost::format("%p:%d") % mt % c));
              }
            }
          }

          vector<string> sst_stat_str;
          {
            lock_guard<mutex> lk2(_sstMapLock2);
            for (auto i: _sstMap) {
              uint64_t sst_id = i.first;
              SstTemp* st = i.second;

              long c = st->GetAndResetNumReads();
              if (c > 0) {
                double dur_since_last_report_simulated_time;
                if (prev_time.is_not_a_date_time()) {
                  dur_since_last_report_simulated_time = REPORT_INTERVAL_SEC_SIMULATED_TIME;
                } else {
                  dur_since_last_report_simulated_time = (cur_time - prev_time).total_nanoseconds()
                    / 1000000000.0 / _simulation_over_simulated_time_dur;
                }
                double reads_per_64MB_per_sec = double(c) / (st->Size() / (64.0*1024*1024)) / dur_since_last_report_simulated_time;

                // Update temperature. You don't need to update st when c != 0.
                st->UpdateTemp(c, reads_per_64MB_per_sec, cur_time);

                sst_stat_str.push_back(str(boost::format("%d:%d:%d:%.3f:%.3f")
                      % sst_id % st->Level() % c % reads_per_64MB_per_sec % st->Temp(cur_time)));
              }
            }
          }

          // Output to the rocksdb log. The condition is just to reduce
          // memt-only logs. So not all memt stats are reported, which is okay.
          if (sst_stat_str.size() > 0) {
            JSONWriter jwriter;
            EventHelpers::AppendCurrentTime(&jwriter);
            jwriter << "mutant_table_acc_cnt";
            jwriter.StartObject();
            if (memt_stat_str.size() > 0)
              jwriter << "memt" << boost::algorithm::join(memt_stat_str, " ");
            if (sst_stat_str.size() > 0)
              jwriter << "sst" << boost::algorithm::join(sst_stat_str, " ");
            jwriter.EndObject();
            jwriter.EndObject();
            _logger->Log(jwriter);
          }

          _updatedSinceLastOutput = false;
        }

        // Update it whether the monitor actually has something to report or not.
        prev_time = cur_time;
        _reported = true;
      }
      _reported_cv.notify_one();
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


// Sleep for REPORT_INTERVAL_SEC_SIMULATED_TIME or until woken up by another thread.
void TabletAccMon::_ReporterSleep() {
  static const auto wait_dur = chrono::milliseconds(int(REPORT_INTERVAL_SEC_SIMULATED_TIME * 1000.0 * _simulation_over_simulated_time_dur));
  //TRACE << boost::format("%d ms\n") % wait_dur.count();
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
void TabletAccMon::SstOpened(BlockBasedTable* bbt, uint64_t size, int level) {
  static TabletAccMon& i = _GetInst();
  i._SstOpened(bbt, size, level);
}


void TabletAccMon::SstClosed(BlockBasedTable* bbt) {
  static TabletAccMon& i = _GetInst();
  i._SstClosed(bbt);
}


void TabletAccMon::ReportAndWait() {
  static TabletAccMon& i = _GetInst();
  i._ReportAndWait();
}


void TabletAccMon::SetUpdated() {
  static TabletAccMon& i = _GetInst();
  i._SetUpdated();
}


//double TabletAccMon::Temperature(uint64_t sst_id, const boost::posix_time::ptime& cur_time) {
//  static TabletAccMon& i = _GetInst();
//  return i._Temperature(sst_id, cur_time);
//}


uint32_t TabletAccMon::CalcOutputPathId(const std::vector<FileMetaData*>& file_metadata) {
  static TabletAccMon& i = _GetInst();
  return i._CalcOutputPathId(file_metadata);
}

}
