#include <atomic>
#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>

#include "db/db_impl.h"
#include "db/event_helpers.h"
#include "db/version_edit.h"
#include "mutant/mutant.h"
#include "table/block_based_table_reader.h"
#include "util/event_logger.h"
#include "util/util.h"

using namespace std;

namespace rocksdb {

// 22.761826, when the simulation time is 60,000 sec and the simulated time is
// 1365709.587 secs.
double _simulation_over_simulated_time_dur = 0.0;

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


Mutant& Mutant::_GetInst() {
  static Mutant i;
  return i;
}


// This object is not released, since it's a singleton object.
Mutant::Mutant()
: _updatedSinceLastOutput(false)
{
}


void Mutant::_Init(const MutantOptions* mo, DBImpl* db, EventLogger* el) {
  // TRACE << boost::format("%d\n%s\n") % std::this_thread::get_id() % Util::StackTrace(1);
  //
  // rocksdb::Mutant::_Init(rocksdb::DBImpl*, rocksdb::EventLogger*)
  // rocksdb::DB::Open(rocksdb::DBOptions const&, std::basic_string<char, std::char_traits<char>, std::allocator<char> > const&,
  //   std::vector<rocksdb::ColumnFamilyDescriptor, std::allocator<rocksdb::ColumnFamilyDescriptor> > const&,
  //   std::vector<rocksdb::ColumnFamilyHandle*, std::allocator<rocksdb::ColumnFamilyHandle*> >*, rocksdb::DB**)
  // rocksdb::DB::Open(rocksdb::Options const&, std::basic_string<char, std::char_traits<char>, std::allocator<char> > const&, rocksdb::DB**)
  // __libc_start_main

  // Make a copy of the options
  if (mo == nullptr)
    THROW("Unexpected");
  _options = *mo;

  if (! _options.mutant_enabled)
    return;

  if (_options.simulation_time_dur_sec == 0.0)
    THROW("Unexpected");
  if (_options.simulated_time_dur_sec == 0.0)
    THROW("Unexpected");

  _simulation_over_simulated_time_dur = _options.simulation_time_dur_sec / _options.simulated_time_dur_sec;

  _db = db;
  _logger = el;

  JSONWriter jwriter;
  EventHelpers::AppendCurrentTime(&jwriter);
  jwriter << "mutant_initialized";
  jwriter.EndObject();
  _logger->Log(jwriter);

  // Let the threads start here! In the constructor, some of the members are not set yet.
  if (_temp_updater_thread)
    THROW("Unexpcted");
  _temp_updater_thread = new thread(bind(&rocksdb::Mutant::_TempUpdaterRun, this));

  // Needed only with migrate_sstables
  if (_options.migrate_sstables)
    _smt_thread = new thread(bind(&rocksdb::Mutant::_SstMigrationTriggererRun, this));
}


void Mutant::_MemtCreated(ColumnFamilyData* cfd, MemTable* m) {
  if (! _options.mutant_enabled)
    return;

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


void Mutant::_MemtDeleted(MemTable* m) {
  if (! _options.mutant_enabled)
    return;

  lock_guard<mutex> lk(_memtSetLock);

  //TRACE << boost::format("MemtDeleted %p\n") % m;

  auto it = _memtSet.find(m);
  if (it == _memtSet.end()) {
    // It happened that the same MemTable (with the same address) was destructed
    // twice, which could be explained by:
    // http://stackoverflow.com/questions/22446562/same-object-deduced-by-memory-address-constructed-twice-without-destruction
    // Ignore for now
    //TRACE << boost::format("%d Interesting the same MemTable is destructed twice\n%s\n")
    //  % std::this_thread::get_id() % Util::StackTrace(1);
  } else {
    // Update temp and report for the last time before erasing the entry
    _RunTempUpdaterAndWait();

    lock_guard<mutex> lk2(_memtSetLock2);
    _memtSet.erase(it);
  }
}


void Mutant::_SstOpened(TableReader* tr, const FileDescriptor* fd, int level) {
  if (! _options.mutant_enabled)
    return;

  lock_guard<mutex> lk(_sstMapLock);

  uint64_t sst_id = fd->GetNumber();
  SstTemp* st = new SstTemp(tr, fd, level);

  //TRACE << boost::format("%d SstOpened sst_id=%d\n") % std::this_thread::get_id() % sst_id;

  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end()) {
    lock_guard<mutex> lk2(_sstMapLock2);
    _sstMap[sst_id] = st;
  } else {
    THROW("Unexpected");
  }
}


void Mutant::_SstClosed(BlockBasedTable* bbt) {
  if (! _options.mutant_enabled)
    return;

  lock_guard<mutex> lk(_sstMapLock);

  uint64_t sst_id = bbt->SstId();
  //TRACE << boost::format("%d _SstClosed sst_id=%d\n") % std::this_thread::get_id() % sst_id;

  //TRACE << boost::format("SstClosed %d\n") % sst_id;
  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end()) {
    // This happens when compaction_readahead_size is specified. Ignore.
    //TRACE << boost::format("%d Hmm... Sst %d closed without having been opened\n") % std::this_thread::get_id() % sst_id;
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


void Mutant::_SetUpdated() {
  if (! _options.mutant_enabled)
    return;

  _updatedSinceLastOutput = true;
}


double Mutant::_Temperature(uint64_t sst_id, const boost::posix_time::ptime& cur_time) {
  lock_guard<mutex> _(_sstMapLock2);
  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end())
    THROW(boost::format("Unexpected: sst_id=%d") % sst_id);
  SstTemp* st = it->second;
  return st->Temp(cur_time);
}


uint32_t Mutant::_CalcOutputPathId(
    bool temperature_triggered_single_sstable_compaction,
    const std::vector<FileMetaData*>& file_metadata) {
  if (! _options.mutant_enabled)
    return 0;
  if (! _options.migrate_sstables)
    return 0;

  if (file_metadata.size() == 0)
    THROW("Unexpected");

  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

  vector<double> input_sst_temp;
  vector<double> input_sst_path_id;
  vector<string> input_sst_info;

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


// Trivial move. db/db_impl.cc:3508
uint32_t Mutant::_CalcOutputPathIdTrivialMove(const FileMetaData* fmd) {
  uint32_t path_id = fmd->fd.GetPathId();

  if (! _options.mutant_enabled)
    return path_id;
  if (! _options.migrate_sstables)
    return path_id;

  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

  uint64_t sst_id = fmd->fd.GetNumber();

  uint32_t output_path_id = path_id;

  // -2.0, when path_id = 1, which can be ignored.
  double temp = -2.0;

  if (path_id == 1) {
    // Cold SSTables stay cold
  } else {
    temp = _Temperature(sst_id, cur_time);
    if ((temp != -1.0) && (temp < SST_TEMP_BECOME_COLD_THRESHOLD)) {
      output_path_id = 1;
    }
  }

  {
    int level;
    {
      lock_guard<mutex> lk2(_sstMapLock2);
      auto it = _sstMap.find(sst_id);
      if (it == _sstMap.end())
        THROW("Unexpected");
      SstTemp* st = it->second;
      level = st->Level();
    }
    string input_sst_info = str(boost::format("sst_id=%d level=%d path_id=%d temp=%.3f")
        % sst_id % level % path_id % temp);

    JSONWriter jwriter;
    EventHelpers::AppendCurrentTime(&jwriter);
    jwriter << "mutant_trivial_move";
    jwriter.StartObject();
    jwriter << "in_sst" << input_sst_info;
    jwriter << "out_sst_path_id" << output_path_id;
    jwriter.EndObject();
    jwriter.EndObject();
    _logger->Log(jwriter);
  }

  return output_path_id;
}


// Returns nullptr when there is no SSTable for migration
FileMetaData* Mutant::_PickColdestSstForMigration(int& level_for_migration) {
  if (! _options.mutant_enabled)
    return nullptr;

  if (! _options.migrate_sstables)
    return nullptr;

  FileMetaData* fmd_with_temp_min = nullptr;

  if (!_db)
    return fmd_with_temp_min;

  // To minimize calling GetMetadataForFile(), we first get a sorted list of
  // sst_ids by their temperature.  GetMetadataForFile() is not very efficient,
  // it has a nested loop.
  //
  // map<temp, sst_id>
  map<double, uint64_t> temp_sstid;

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

    double temp = st->Temp(cur_time);
    if ((temp != TEMP_UNINITIALIZED) && (temp < SST_TEMP_BECOME_COLD_THRESHOLD))
      temp_sstid[temp] = sst_id;
  }

  for (auto i: temp_sstid) {
    uint64_t sst_id = i.second;

    int filelevel;
    FileMetaData* fmd;
    ColumnFamilyData* cfd;
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

    fmd_with_temp_min = fmd;
    level_for_migration = filelevel;
  }

  // There is no guarantee that the fmd points to a live SSTable after it is
  // returned. I think the sames goes with the regular compaction picking path.
  // Hope the compaction implementation takes care of it.
  return fmd_with_temp_min;
}


void Mutant::_TempUpdaterRun() {
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
void Mutant::_TempUpdaterSleep() {
  static const auto wait_dur = chrono::milliseconds(long(TEMP_UPDATE_INTERVAL_SEC_SIMULATED_TIME * 1000.0 * _simulation_over_simulated_time_dur));
  //TRACE << boost::format("%d ms\n") % wait_dur.count();
  unique_lock<mutex> lk(_temp_updater_sleep_mutex);
  _temp_updater_sleep_cv.wait_for(lk, wait_dur, [&](){return _temp_updater_wakeupnow;});
  _temp_updater_wakeupnow = false;
}


void Mutant::_TempUpdaterWakeup() {
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
void Mutant::_RunTempUpdaterAndWait() {
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
void Mutant::_SstMigrationTriggererRun() {
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
      // later in _PickColdestSstForMigration().
      bool may_have_sstable_to_migrate = false;
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
            may_have_sstable_to_migrate = true;
            break;
          }
        }
      }

      if (may_have_sstable_to_migrate)
        _db->MutantMayScheduleCompaction(_cfd);
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


// These 2 functions are not called when mutant_enabled = false
void Mutant::_SstMigrationTriggererSleep() {
  static const auto wait_dur = chrono::milliseconds(long(SST_MIGRATION_TRIGGERER_INTERVAL_SIMULATED_TIME_SEC * 1000.0 * _simulation_over_simulated_time_dur));
  unique_lock<mutex> lk(_smt_sleep_mutex);
  _smt_sleep_cv.wait_for(lk, wait_dur, [&](){return _smt_wakeupnow;});
  _smt_wakeupnow = false;
}


void Mutant::_SstMigrationTriggererWakeup() {
  {
    lock_guard<mutex> _(_smt_sleep_mutex);
    _smt_wakeupnow = true;
  }
  _smt_sleep_cv.notify_one();
}


void Mutant::_Shutdown() {
  if (! _options.mutant_enabled)
    return;

  _smt_stop_requested = true;
  _SstMigrationTriggererWakeup();
  if (_smt_thread) {
    _smt_thread->join();
    delete _smt_thread;
    _smt_thread = nullptr;
  }

  {
    lock_guard<mutex> _(_temp_updating_mutex);
    _temp_updater_stop_requested = true;
    _TempUpdaterWakeup();
  }
  if (_temp_updater_thread) {
    _temp_updater_thread->join();
    delete _temp_updater_thread;
    _temp_updater_thread = nullptr;
  }
}


void Mutant::Init(const MutantOptions* mo, DBImpl* db, EventLogger* el) {
  static Mutant& i = _GetInst();
  i._Init(mo, db, el);
}


void Mutant::MemtCreated(ColumnFamilyData* cfd, MemTable* m) {
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

  static Mutant& i = _GetInst();
  i._MemtCreated(cfd, m);
}


void Mutant::MemtDeleted(MemTable* m) {
  static Mutant& i = _GetInst();
  i._MemtDeleted(m);
}


// BlockBasedTable() calls this.
//
// FileDescriptor constructor/destructor was not good ones to monitor. They
// were copyable and the same address was opened and closed multiple times.
//
// It is called when a SSTable is opened, created from flush and created from
// compaction.  See the comment in BlockBasedTable::BlockBasedTable().
void Mutant::SstOpened(TableReader* tr, const FileDescriptor* fd, int level) {
  // TRACE << Util::StackTrace(1) << "\n";
  //
  // rocksdb::Mutant::SstOpened(rocksdb::BlockBasedTable*, rocksdb::FileDescriptor const*, int)
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

  static Mutant& i = _GetInst();
  i._SstOpened(tr, fd, level);
}


void Mutant::SstClosed(BlockBasedTable* bbt) {
  static Mutant& i = _GetInst();
  i._SstClosed(bbt);
}


void Mutant::SetUpdated() {
  static Mutant& i = _GetInst();
  i._SetUpdated();
}


uint32_t Mutant::CalcOutputPathId(
    bool temperature_triggered_single_sstable_compaction,
    const std::vector<FileMetaData*>& file_metadata) {
  static Mutant& i = _GetInst();
  return i._CalcOutputPathId(temperature_triggered_single_sstable_compaction, file_metadata);
}


uint32_t Mutant::CalcOutputPathIdTrivialMove(const FileMetaData* fmd) {
  static Mutant& i = _GetInst();
  return i._CalcOutputPathIdTrivialMove(fmd);
}


FileMetaData* Mutant::PickColdestSstForMigration(int& level_for_migration) {
  static Mutant& i = _GetInst();
  return i._PickColdestSstForMigration(level_for_migration);
}


void Mutant::Shutdown() {
  static Mutant& i = _GetInst();
  i._Shutdown();
}

}
