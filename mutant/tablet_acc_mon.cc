#include <atomic>
#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>

#include "db/db_impl.h"
#include "db/event_helpers.h"
#include "db/version_edit.h"
#include "mutant/tablet_acc_mon.h"
#include "table/block_based_table_reader.h"
#include "util/event_logger.h"
#include "util/util.h"

using namespace std;

namespace rocksdb {

// Note: make these configurable
//const double _simulation_time_dur_sec =   60000.0  ;
// For fast dev. On mjolnir, without limiting memory:
//   With 6000 secs, about 90% CPU idle.
//   With 4000 secs, about 85% CPU idle.
//   With 2000 secs, about 55% CPU idle. I think this is good enough.

// Working on multi db_paths. Let's go a bit faster.
const double _simulation_time_dur_sec =   1500.0  ;

const double _simulated_time_dur_sec  = 1365709.587;
const double _simulation_over_simulated_time_dur = _simulation_time_dur_sec / _simulated_time_dur_sec;
// 22.761826, with the 60,000 sec simulation time

const double TEMP_UNINITIALIZED = -1.0;
const double TEMP_DECAY_FACTOR = 0.999;
const double SST_TEMP_BECOME_COLD_THRESHOLD = 20.0;
const long TEMP_UPDATE_INTERVAL_SEC_SIMULATED_TIME = 60;
const long SST_MIGRATION_TRIGGERER_INTERVAL_SIMULATED_TIME_SEC = 60;


class SstTemp {
  TableReader* _tr;
  boost::posix_time::ptime _created;

  // _level is always set. It used to be -1 sometimes, but not any more after
  // updating the code in CompactionJob::FinishCompactionOutputFile().
  int _level;

  uint64_t _sst_id;
  uint64_t _size;
  uint32_t _path_id;
  long _initial_reads = 0;

  double _temp = TEMP_UNINITIALIZED;
  boost::posix_time::ptime _last_updated;

  // Let's not complicate it for now.
  // May want to define 4 different levels.
  // For now just 2:
  //   0: cold
  //   1: hot
  //int _temp_level = 0;
  //boost::posix_time::ptime _became_cold_at_simulation_time;
  //bool _became_cold_at_defined = false;

public:
  // This is called by _SstOpened() with the locks (_sstMapLock, _sstMapLock2) held.
  SstTemp(TableReader* tr, const FileDescriptor* fd, int level)
  : _tr(tr)
    , _created(boost::posix_time::microsec_clock::local_time())
    , _level(level)
  {
    _sst_id = fd->GetNumber();

    // Keep a copy of _size here. fd seems to change over the life of a SstTemp.
    // The same goes with the other members from fd.
    _size = fd->GetFileSize();
    if (_size == 0)
      THROW("Unexpected");

    _path_id = fd->GetPathId();
  }

  long GetAndResetNumReads() {
    return _tr->GetAndResetNumReads();
  }

  // This is called with _sstMapLock2 held.
  void UpdateTemp(long reads, double reads_per_64MB_per_sec, const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
      // When you get a first accfreq, assume the same amount of accesses have
      // been there before for an infinite amount of time.
      //
      // This might cause a peak in the beginning and prevent a compaction
      // including this one from generating cold SSTables, but that's ok.
      _temp = reads_per_64MB_per_sec;
      _last_updated = t;
    } else {
      double dur_since_last_update_in_sec_simulated_time = (t - _last_updated).total_nanoseconds()
        / 1000000000.0 / _simulation_over_simulated_time_dur;
      // Assume the reads, thus the temperature change, happen in the middle of
      // (_last_updated, t), thus the / 2.0.
      _temp = _temp * pow(TEMP_DECAY_FACTOR, dur_since_last_update_in_sec_simulated_time)
        + reads / (_size/(64.0*1024*1024))
        * pow(TEMP_DECAY_FACTOR, dur_since_last_update_in_sec_simulated_time / 2.0)
        * (1 - TEMP_DECAY_FACTOR);

      // Not sure if there is any SSTable that becomes "hotter" natually.
      // QuizUp trace doesn't. Synthetic ones may.

      _last_updated = t;
    }
  }

  // This is called with _sstMapLock2 held.
  double Temp(const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
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

  uint32_t PathId() {
    return _path_id;
  }
};


TabletAccMon& TabletAccMon::_GetInst() {
  static TabletAccMon i;
  return i;
}


// This object is not released, since it's a singleton object.
TabletAccMon::TabletAccMon()
: _updatedSinceLastOutput(false)
  , _temp_updater_thread(thread(bind(&rocksdb::TabletAccMon::_TempUpdaterRun, this)))
  , _smt_thread(thread(bind(&rocksdb::TabletAccMon::_SstMigrationTriggererRun, this)))
{
}


void TabletAccMon::_Init(DBImpl* db, EventLogger* el) {
  _db = db;
  _logger = el;
  //TRACE << "TabletAccMon initialized\n";

  JSONWriter jwriter;
  EventHelpers::AppendCurrentTime(&jwriter);
  jwriter << "mutant_table_acc_mon_init";
  jwriter.EndObject();
  _logger->Log(jwriter);
}


void TabletAccMon::_MemtCreated(ColumnFamilyData* cfd, MemTable* m) {
  lock_guard<mutex> lk(_memtSetLock);

  if (_cfd == nullptr) {
    _cfd = cfd;
  } else {
    if (_cfd != cfd)
      THROW("Unexpected");
  }

  //TRACE << boost::format("MemtCreated %p\n") % m;

  auto it = _memtSet.find(m);
  if (it == _memtSet.end()) {
    lock_guard<mutex> lk2(_memtSetLock2);
    _memtSet.insert(m);
    // Note: Log creation/deletion time, if needed. You don't need to wake up
    // the temp_updater thread.
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
    // Update temp and report for the last time before erasing the entry
    _RunTempUpdaterAndWait();

    lock_guard<mutex> lk2(_memtSetLock2);
    _memtSet.erase(it);
  }
}


void TabletAccMon::_SstOpened(TableReader* tr, const FileDescriptor* fd, int level) {
  lock_guard<mutex> lk(_sstMapLock);

  uint64_t sst_id = fd->GetNumber();
  SstTemp* st = new SstTemp(tr, fd, level);

  //TRACE << boost::format("SstOpened sst_id=%d\n") % sst_id;

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
    TRACE << boost::format("Hmm... Sst %d closed without having been opened\n") % sst_id;
    //THROW("Unexpected");
  } else {
    // Update temp and report for the last time before erasing the entry.
    _RunTempUpdaterAndWait();

    // You need a 2-level locking with _sstMapLock and _sstMapLock2.
    // _RunTempUpdaterAndWait() waits for the temp_updater thread to finish a cycle, which
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


uint32_t TabletAccMon::_CalcOutputPathId(
    bool temperature_triggered_single_sstable_compaction,
    const std::vector<FileMetaData*>& file_metadata,
    vector<string>& input_sst_info) {
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

    int level;
    {
      lock_guard<mutex> lk2(_sstMapLock2);
      auto it = _sstMap.find(sst_id);
      if (it == _sstMap.end())
        THROW("Unexpected");
      SstTemp* st = it->second;
      level = st->Level();
    }

    input_sst_info.push_back(str(boost::format("(sst_id=%d level=%d path_id=%d temp=%.3f)")
          % sst_id % level % path_id % temp));
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

  {
    JSONWriter jwriter;
    EventHelpers::AppendCurrentTime(&jwriter);
    jwriter << "mutant_tablet_compaction";
    jwriter.StartObject();
    jwriter << "temp_triggered_single_sst_compaction" << temperature_triggered_single_sstable_compaction;
    if (input_sst_info.size() > 0)
      jwriter << "in_sst" << boost::algorithm::join(input_sst_info, " ");
    jwriter << "out_sst_path_id" << output_path_id;
    jwriter.EndObject();
    jwriter.EndObject();
    _logger->Log(jwriter);
  }

  return output_path_id;
}


uint32_t TabletAccMon::_CalcOutputPathId(const FileMetaData* fmd) {
  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

  uint64_t sst_id = fmd->fd.GetNumber();
  uint32_t path_id = fmd->fd.GetPathId();
  double temp = _Temperature(sst_id, cur_time);

  TRACE << boost::format("%d Input Sst: sst_id=%d path_id=%d temp=%.3f\n")
    % std::this_thread::get_id() % sst_id % path_id % temp;

  uint32_t output_path_id = path_id;

  if ((temp != -1.0) && (temp < SST_TEMP_BECOME_COLD_THRESHOLD)) {
    output_path_id = 1;
  }
  TRACE << boost::format("%d Output Sst path_id=%d\n")
    % std::this_thread::get_id() % output_path_id;
  return output_path_id;
}


FileMetaData* TabletAccMon::_PickSstForMigration(int& level_for_migration) {
  double temp_min = -1.0;
  // nullptr is for no SSTable suitable for migration
  FileMetaData* fmd_with_temp_min = nullptr;

  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

  lock_guard<mutex> _(_sstMapLock2);
  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    SstTemp* st = i.second;
    int level = i.second->Level();
    // We don't consider level -1 (there used to be, not anymore) and 0.
    if (level <= 0)
      continue;

    // TODO: Revisit this for multi-level path_ids. 2-level for now.
    if (st->PathId() > 0)
      continue;

    if (!_db)
      return fmd_with_temp_min;

    int filelevel;
    FileMetaData* fmd;
    ColumnFamilyData* cfd;
    // TODO: Not very efficient. GetMetadataForFile() has a nested loop.
    Status s = _db->MutantGetMetadataForFile(sst_id, &filelevel, &fmd, &cfd);
    if (s.code() == Status::kNotFound) {
      // This rarely happens, but happens. Ignore the sst.
      continue;
    } else {
      if (!s.ok())
        THROW(boost::format("Unexpected: s=%s sst_id=%d") % s.ToString() % sst_id);
    }

    if (fmd->being_compacted)
      continue;

    double temp = st->Temp(cur_time);
    if ((temp != TEMP_UNINITIALIZED) && (temp < SST_TEMP_BECOME_COLD_THRESHOLD)) {
      if (fmd_with_temp_min == nullptr) {
        temp_min = temp;
        fmd_with_temp_min = fmd;
        level_for_migration = level;
      } else {
        if (temp < temp_min) {
          temp_min = temp;
          fmd_with_temp_min = fmd;
          level_for_migration = level;
        }
      }
    }
  }

  return fmd_with_temp_min;
}


void TabletAccMon::_TempUpdaterRun() {
  try {
    boost::posix_time::ptime prev_time;

    while (! _temp_updater_stop_requested) {
      _TempUpdaterSleep();

      {
        lock_guard<mutex> lk(_temp_updating_mutex);

        boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

        // A 2-level check. _temp_updater_sleep_cv alone is not enough, since this
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
                double dur_since_last_update_simulated_time;
                if (prev_time.is_not_a_date_time()) {
                  // No need to worry about the time duration and the
                  // inaccuracy.  This happens only once right after RocksDB is
                  // initialized.
                  dur_since_last_update_simulated_time = TEMP_UPDATE_INTERVAL_SEC_SIMULATED_TIME;
                } else {
                  dur_since_last_update_simulated_time = (cur_time - prev_time).total_nanoseconds()
                    / 1000000000.0 / _simulation_over_simulated_time_dur;
                }
                double reads_per_64MB_per_sec = double(c) / (st->Size() / (64.0*1024*1024)) / dur_since_last_update_simulated_time;

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

        // Set updated whether or not the monitor actually has something to update.
        prev_time = cur_time;
        _temp_updated = true;
      }
      // There can be multiple threads waiting for this. Notify them all.
      _temp_updated_cv.notify_all();
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


// Sleep for TEMP_UPDATE_INTERVAL_SEC_SIMULATED_TIME or until woken up by another thread.
void TabletAccMon::_TempUpdaterSleep() {
  static const auto wait_dur = chrono::milliseconds(long(TEMP_UPDATE_INTERVAL_SEC_SIMULATED_TIME * 1000.0 * _simulation_over_simulated_time_dur));
  //TRACE << boost::format("%d ms\n") % wait_dur.count();
  unique_lock<mutex> lk(_temp_updater_sleep_mutex);
  _temp_updater_sleep_cv.wait_for(lk, wait_dur, [&](){return _temp_updater_wakeupnow;});
  _temp_updater_wakeupnow = false;
}


void TabletAccMon::_TempUpdaterWakeup() {
  // This wakes up the waiting thread even if this is called before wait.
  // _temp_updater_wakeupnow does the magic.
  {
    lock_guard<mutex> lk(_temp_updater_sleep_mutex);
    _temp_updater_wakeupnow = true;
  }
  _temp_updater_sleep_cv.notify_one();
}


// Run a cycle of temp updater and wait.  This is called when a Memtable or
// SSTable is about to be gone.  This is not to miss any updates on them. You
// never know if there will be a spike at the end of the lifetime.
void TabletAccMon::_RunTempUpdaterAndWait() {
  {
    lock_guard<mutex> _(_temp_updating_mutex);
    _temp_updated = false;
  }

  // Force wakeup to minimize the wait time.
  _TempUpdaterWakeup();

  // Wait for the temp_updater to finish a update cycle
  {
    unique_lock<mutex> _(_temp_updating_mutex);
    if (! _temp_updater_stop_requested) {
      _temp_updated_cv.wait(_, [&](){return _temp_updated;});
    }
  }
}


// This thread schedules a background SSTable compaction when there may be a
// cold SSTable.
//
// When you schedule a background compaction and there is no compaction to do,
// you get "Compaction nothing to do".
void TabletAccMon::_SstMigrationTriggererRun() {
  try {
    while (! _smt_stop_requested) {
      _SstMigrationTriggererSleep();

      if (!_cfd)
        continue;

      if (_db->UnscheduledCompactions() > 0)
        continue;

      // Schedule only when there is a cold SSTable to avoid scheduling too
      // many compactions that end up doing nothing.
      //
      // A simpler, is-there-any-cold-SSTable check. The full check is done
      // later in _PickSstForMigration().
      bool cold_sstable_may_exist = false;
      boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();
      {
        lock_guard<mutex> _(_sstMapLock2);
        for (auto i: _sstMap) {
          SstTemp* st = i.second;
          int level = i.second->Level();
          // We don't consider level -1 (there used to be, not anymore) and 0.
          if (level <= 0)
            continue;

          // TODO: Revisit this for multi-level path_ids. 2-level for now.
          if (st->PathId() > 0)
            continue;

          double temp = st->Temp(cur_time);
          if ((temp != TEMP_UNINITIALIZED) && (temp < SST_TEMP_BECOME_COLD_THRESHOLD)) {
            cold_sstable_may_exist = true;
            break;
          }
        }
      }

      if (cold_sstable_may_exist)
        _db->MutantMayScheduleCompaction(_cfd);
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


void TabletAccMon::_SstMigrationTriggererSleep() {
  static const auto wait_dur = chrono::milliseconds(long(SST_MIGRATION_TRIGGERER_INTERVAL_SIMULATED_TIME_SEC * 1000.0 * _simulation_over_simulated_time_dur));
  unique_lock<mutex> lk(_smt_sleep_mutex);
  _smt_sleep_cv.wait_for(lk, wait_dur, [&](){return _smt_wakeupnow;});
  _smt_wakeupnow = false;
}


void TabletAccMon::_SstMigrationTriggererWakeup() {
  {
    lock_guard<mutex> _(_smt_sleep_mutex);
    _smt_wakeupnow = true;
  }
  _smt_sleep_cv.notify_one();
}


void TabletAccMon::_Shutdown() {
  _smt_stop_requested = true;
  _SstMigrationTriggererWakeup();
  _smt_thread.join();

  {
    lock_guard<mutex> _(_temp_updating_mutex);
    _temp_updater_stop_requested = true;
    _TempUpdaterWakeup();
  }
  _temp_updater_thread.join();
}


void TabletAccMon::Init(DBImpl* db, EventLogger* el) {
  static TabletAccMon& i = _GetInst();
  i._Init(db, el);
}


void TabletAccMon::MemtCreated(ColumnFamilyData* cfd, MemTable* m) {
  // This is called more often than SstOpened() at least in the beginning of
  // the experiment. I think it won't be less often even when the DB restarts.
  //
  //TRACE << boost::format("%d cfd=%p cfd_name=%s\n") % std::this_thread::get_id() % cfd % cfd->GetName();

  if (cfd->GetName() != "default") {
    // Are there any CF other than the user-provided, "default"? If not,
    // monitoring becomes a bit easier.  Cassandra has system CF.
    TRACE << boost::format("%d Interesting! cfd=%p cfd_name=%s\n")
      % std::this_thread::get_id() % cfd % cfd->GetName();
    return;
  }

  // TRACE << Util::StackTrace(1) << "\n";
  //
  // rocksdb::MemTable::MemTable(rocksdb::InternalKeyComparator const&, rocksdb::ImmutableCFOptions const&, rocksdb::MutableCFOptions const&, rocksdb::WriteBufferManager*, unsigned long)
  // rocksdb::ColumnFamilyData::ConstructNewMemtable(rocksdb::MutableCFOptions const&, unsigned long)
  //   You can get ColumnFamilyData* here
  //
  // rocksdb::DBImpl::SwitchMemtable(rocksdb::ColumnFamilyData*, rocksdb::DBImpl::WriteContext*)
  // rocksdb::DBImpl::ScheduleFlushes(rocksdb::DBImpl::WriteContext*)
  // rocksdb::DBImpl::WriteImpl(rocksdb::WriteOptions const&, rocksdb::WriteBatch*, rocksdb::WriteCallback*, unsigned long*, unsigned long, bool)
  // rocksdb::DBImpl::Write(rocksdb::WriteOptions const&, rocksdb::WriteBatch*)
  // rocksdb::DB::Put(rocksdb::WriteOptions const&, rocksdb::ColumnFamilyHandle*, rocksdb::Slice const&, rocksdb::Slice const&)
  // rocksdb::DBImpl::Put(rocksdb::WriteOptions const&, rocksdb::ColumnFamilyHandle*, rocksdb::Slice const&, rocksdb::Slice const&)
  // rocksdb::DB::Put(rocksdb::WriteOptions const&, rocksdb::Slice const&, rocksdb::Slice const&)

  static TabletAccMon& i = _GetInst();
  i._MemtCreated(cfd, m);
}


void TabletAccMon::MemtDeleted(MemTable* m) {
  static TabletAccMon& i = _GetInst();
  i._MemtDeleted(m);
}


// BlockBasedTable() calls this.
//
// FileDescriptor constructor/destructor was not good ones to monitor. They
// were copyable and the same address was opened and closed multiple times.
//
// It is called when a SSTable is opened, created from flush and created from
// compaction.  See the comment in BlockBasedTable::BlockBasedTable().
void TabletAccMon::SstOpened(TableReader* tr, const FileDescriptor* fd, int level) {
  // TRACE << Util::StackTrace(1) << "\n";
  //
  // rocksdb::TabletAccMon::SstOpened(rocksdb::BlockBasedTable*, rocksdb::FileDescriptor const*, int)
  // rocksdb::BlockBasedTable::Open(rocksdb::ImmutableCFOptions const&, rocksdb::EnvOptions const&, rocksdb::BlockBasedTableOptions const&, rocksdb::InternalKeyComparator const&, std::unique_ptr<rocksdb::RandomAccessFileReader, std::default_delete<rocksdb::RandomAccessFileReader> >&&, unsigned long, std::unique_ptr<rocksdb::TableReader, std::default_delete<rocksdb::TableReader> >*, rocksdb::FileDescriptor const*, bool, bool, int)
  // rocksdb::BlockBasedTableFactory::NewTableReader(rocksdb::TableReaderOptions const&, std::unique_ptr<rocksdb::RandomAccessFileReader, std::default_delete<rocksdb::RandomAccessFileReader> >&&, unsigned long, std::unique_ptr<rocksdb::TableReader, std::default_delete<rocksdb::TableReader> >*, rocksdb::FileDescriptor const*, bool) const
  // rocksdb::TableCache::GetTableReader(rocksdb::EnvOptions const&, rocksdb::InternalKeyComparator const&, rocksdb::FileDescriptor const&, bool, unsigned long, bool, rocksdb::HistogramImpl*, std::unique_ptr<rocksdb::TableReader, std::default_delete<rocksdb::TableReader> >*, bool, int, bool)
  // rocksdb::TableCache::FindTable(rocksdb::EnvOptions const&, rocksdb::InternalKeyComparator const&, rocksdb::FileDescriptor const&, rocksdb::Cache::Handle**, bool, bool, rocksdb::HistogramImpl*, bool, int, bool)
  // rocksdb::TableCache::NewIterator(rocksdb::ReadOptions const&, rocksdb::EnvOptions const&, rocksdb::InternalKeyComparator const&, rocksdb::FileDescriptor const&, rocksdb::TableReader**, rocksdb::HistogramImpl*, bool, rocksdb::Arena*, bool, int)
  // rocksdb::CompactionJob::FinishCompactionOutputFile(rocksdb::Status const&, rocksdb::CompactionJob::SubcompactionState*)
  //   You can get ColumnFamilyData* in FinishCompactionOutputFile()
  //
  // rocksdb::CompactionJob::ProcessKeyValueCompaction(rocksdb::CompactionJob::SubcompactionState*)
  // rocksdb::CompactionJob::Run()
  // rocksdb::DBImpl::BackgroundCompaction(bool*, rocksdb::JobContext*, rocksdb::LogBuffer*, void*)
  // rocksdb::DBImpl::BackgroundCallCompaction(void*)
  // rocksdb::ThreadPool::BGThread(unsigned long)

  static TabletAccMon& i = _GetInst();
  i._SstOpened(tr, fd, level);
}


void TabletAccMon::SstClosed(BlockBasedTable* bbt) {
  static TabletAccMon& i = _GetInst();
  i._SstClosed(bbt);
}


void TabletAccMon::SetUpdated() {
  static TabletAccMon& i = _GetInst();
  i._SetUpdated();
}


uint32_t TabletAccMon::CalcOutputPathId(
    bool temperature_triggered_single_sstable_compaction,
    const std::vector<FileMetaData*>& file_metadata,
    vector<string>& input_sst_info) {
  static TabletAccMon& i = _GetInst();
  return i._CalcOutputPathId(temperature_triggered_single_sstable_compaction, file_metadata, input_sst_info);
}


uint32_t TabletAccMon::CalcOutputPathId(const FileMetaData* fmd) {
  static TabletAccMon& i = _GetInst();
  return i._CalcOutputPathId(fmd);
}


FileMetaData* TabletAccMon::PickSstForMigration(int& level_for_migration) {
  static TabletAccMon& i = _GetInst();
  return i._PickSstForMigration(level_for_migration);
}


void TabletAccMon::Shutdown() {
  static TabletAccMon& i = _GetInst();
  i._Shutdown();
}

}
