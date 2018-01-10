#include <atomic>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/format.hpp>
#include <boost/range/adaptor/transformed.hpp>

#include "db/db_impl.h"
#include "db/event_helpers.h"
#include "db/version_edit.h"
#include "mutant/mutant.h"
#include "table/block_based_table_reader.h"
#include "util/event_logger.h"
#include "util/util.h"

using namespace std;

namespace rocksdb {

const double TEMP_UNINITIALIZED = -1.0;
double TEMP_DECAY_FACTOR = 0.999;

// There are in realtime when not replaying a workload in the past, and in simulated time when replaying one.
long _temp_update_interval_sec;
long _sst_migration_triggerer_interval_sec;

// With the Quizup trace, 22.761826: the simulation time is 60,000 secs and the simulated time is 1365709.587 secs, almost 16 days.
double _simulation_time_dur_over_simulated = 0.0;

// In sec
const long YOUNG_SST_AGE_CUTOFF = 30;


// SSTable and its temperature
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
  // This is called by _SstOpened() with the locks (_sstMapLock_OpenedClosed, _sstMapLock) held.
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

  // This is called with _sstMapLock held.
  void UpdateTemp(long reads, double reads_per_64MB_per_sec, const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
      // When you get a first accfreq, assume the same amount of accesses have
      // been there before for an infinite amount of time.
      //
      // This cause a peak when the SSTable is created, which is ok. The SSTable is just super hot.
      _temp = reads_per_64MB_per_sec;
      _last_updated = t;
    } else {
      double dur_since_last_update_in_sec_simulated_time = (t - _last_updated).total_nanoseconds()
        / 1000000000.0 / _simulation_time_dur_over_simulated;
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

  // This is called with _sstMapLock held.
  double Temp(const boost::posix_time::ptime& t) {
    if (_temp == TEMP_UNINITIALIZED) {
      return _temp;
    } else {
      return _temp * pow(TEMP_DECAY_FACTOR, (t - _last_updated).total_nanoseconds()
          / 1000000000.0 / _simulation_time_dur_over_simulated);
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

  // Returns the current SSTable age. Used for SSTable organization
  long Age(const boost::posix_time::ptime& cur_time) {
    return (cur_time - _created).total_seconds();
  }
};


Mutant& Mutant::_GetInst() {
  // The object instance won't be released since it's static, singleton.
  static Mutant i;
  return i;
}


Mutant::Mutant()
{
}


void Mutant::_Init(const DBOptions::MutantOptions* mo, DBImpl* db, EventLogger* el) {
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

  if (! _options.monitor_temp)
    return;

  TEMP_DECAY_FACTOR = _options.temp_decay_factor;

  _db = db;
  _logger = el;
  if (! _logger)
    THROW("Unexpcted");

  if (_options.replaying) {
    _temp_update_interval_sec = 60;
    _sst_migration_triggerer_interval_sec = 60;

    if (_options.simulation_time_dur_sec == 0.0)
      THROW("Unexpected");
    if (_options.simulated_time_dur_sec == 0.0)
      THROW("Unexpected");

    _simulation_time_dur_over_simulated = _options.simulation_time_dur_sec / _options.simulated_time_dur_sec;
  } else {
    // 1 sec in real time
    _temp_update_interval_sec = 1;
    _sst_migration_triggerer_interval_sec = 1;
    _simulation_time_dur_over_simulated = 1.0;
  }

  JSONWriter jwriter;
  EventHelpers::AppendCurrentTime(&jwriter);
  jwriter << "mutant_initialized";
  jwriter.EndObject();
  _logger->Log(jwriter);

  // The threads start here, not in the constructor, where some of the members are not set yet.
  if (_temp_updater_thread)
    THROW("Unexpcted");
  _temp_updater_thread = new thread(bind(&rocksdb::Mutant::_TempUpdaterRun, this));

  // _smt_thread is needed only with calc_sst_placement
  if (_options.calc_sst_placement) {
    if (_smt_thread)
      THROW("Unexpcted");

    size_t num_stgs = _options.stg_cost_list.size();
    if (num_stgs == 1) {
      // No need for migration. All SSTables go to the first, the only storage.
    } else if (num_stgs == 2) {
      if (! ((_options.stg_cost_list[1] <= _options.stg_cost_slo) && (_options.stg_cost_slo <= _options.stg_cost_list[0])))
        THROW(boost::format("Unexpected: %f %f %f") % _options.stg_cost_list[0] % _options.stg_cost_list[1] % _options.stg_cost_slo);

      // Do the knapsack-based SSTable organization.
      _smt_thread = new thread(bind(&rocksdb::Mutant::_SstMigrationTriggererRun, this));
    } else {
      THROW(boost::format("Unexpected: %d") % num_stgs);
    }
  }

  _initialized = true;
}


void Mutant::_MemtCreated(ColumnFamilyData* cfd, MemTable* m) {
  if (! _initialized)
    return;
  if (! _options.monitor_temp)
    return;

  lock_guard<mutex> lk(_memtSetLock_OpenedClosed);

  if (cfd->GetName() != "usertable") {
    if (cfd->GetName() != "default")
      TRACE << boost::format("%d Interesting! cfd=%p cfd_name=%s\n") % std::this_thread::get_id() % cfd % cfd->GetName();
    return;
  }

  if (_cfd == nullptr) {
    _cfd = cfd;
  } else {
    if (_cfd != cfd)
      THROW(boost::format("Unexpected: %p %s %p %s") % _cfd % _cfd->GetName() % cfd % cfd->GetName());
  }

  //TRACE << boost::format("MemtCreated %p\n") % m;

  auto it = _memtSet.find(m);
  if (it == _memtSet.end()) {
    lock_guard<mutex> lk2(_memtSetLock);
    _memtSet.insert(m);
    // Note: Log creation/deletion time, if needed. You don't need to wake up
    // the temp_updater thread.
  } else {
    THROW("Unexpected");
  }
}


void Mutant::_MemtDeleted(MemTable* m) {
  if (! _initialized)
    return;
  if (! _options.monitor_temp)
    return;

  lock_guard<mutex> lk(_memtSetLock_OpenedClosed);

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

    lock_guard<mutex> lk2(_memtSetLock);
    _memtSet.erase(it);
  }
}


void Mutant::_SstOpened(TableReader* tr, const FileDescriptor* fd, int level) {
  if (! _options.monitor_temp)
    return;
  if (! _initialized) {
    TRACE << "Interesting...\n";
    return;
  }

  lock_guard<mutex> lk(_sstMapLock_OpenedClosed);

  uint64_t sst_id = fd->GetNumber();
  SstTemp* st = new SstTemp(tr, fd, level);

  //TRACE << boost::format("%d SstOpened sst_id=%d\n") % std::this_thread::get_id() % sst_id;

  lock_guard<mutex> lk2(_sstMapLock);
  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end()) {
    _sstMap[sst_id] = st;
  } else {
    // This happens. Interesting. Update the map.
    delete it->second;
    _sstMap[sst_id] = st;
    TRACE << boost::format("%d Interesting! sst_id=%d\n%s\n") % std::this_thread::get_id() % sst_id % Util::StackTrace(1);
  }
}


void Mutant::_SstClosed(BlockBasedTable* bbt) {
  if (! _initialized)
    return;
  if (! _options.monitor_temp)
    return;

  lock_guard<mutex> lk(_sstMapLock_OpenedClosed);

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

    // You need a 2-level locking with _sstMapLock_OpenedClosed and _sstMapLock.
    //   _RunTempUpdaterAndWait() waits for the temp_updater thread to finish a cycle, which
    //   acquires _sstMapLock. The same goes with _memtSetLock.
    lock_guard<mutex> lk2(_sstMapLock);
    delete it->second;
    _sstMap.erase(it);
  }
}


void Mutant::_SetUpdated() {
  if (! _initialized)
    return;
  if (! _options.monitor_temp)
    return;

  _updatedSinceLastOutput = true;
}


double Mutant::_SstTemperature(uint64_t sst_id, const boost::posix_time::ptime& cur_time) {
  lock_guard<mutex> _(_sstMapLock);
  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end()) {
    //THROW(boost::format("Unexpected: sst_id=%d") % sst_id);
    // This happens when you open an existing database and a compaction is done right away.
    // Return -1.0, undefined.
    return -1.0;
  }
  SstTemp* st = it->second;
  return st->Temp(cur_time);
}


int Mutant::_SstLevel(uint64_t sst_id) {
  lock_guard<mutex> _(_sstMapLock);

  int level = -1;
  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end()) {
    TRACE << boost::format("Interesting: sst_id=%d not in _sstMap. Returning level -1") % sst_id;
  } else {
    SstTemp* st = it->second;
    level = st->Level();
  }
  return level;
}


double Mutant::_SstTemp(uint64_t sst_id) {
  lock_guard<mutex> _(_sstMapLock);

  double temp = -1;
  auto it = _sstMap.find(sst_id);
  if (it == _sstMap.end()) {
    TRACE << boost::format("Interesting: sst_id=%d not in _sstMap. Returning temp -1") % sst_id;
  } else {
    SstTemp* st = it->second;
    boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();
    temp = st->Temp(cur_time);
  }
  return temp;
}


uint32_t Mutant::_CalcOutputPathId(
    bool temperature_triggered_single_sstable_compaction,
    const std::vector<FileMetaData*>& file_metadata,
    int output_level) {
  // Return the first storage device when migration is not wanted.
  if (! _initialized)
    return 0;
  if (! _options.monitor_temp)
    return 0;
  if (! _options.calc_sst_placement)
    return 0;

  if (file_metadata.size() == 0)
    THROW("Unexpected");

  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

  vector<double> input_sst_temp;
  vector<double> input_sst_path_id;
  vector<string> input_sst_info;

  for (const auto& fmd: file_metadata) {
    if (fmd == nullptr)
      THROW("Interesting");

    uint64_t sst_id = fmd->fd.GetNumber();
    uint32_t path_id = fmd->fd.GetPathId();

    double temp = TEMP_UNINITIALIZED;
    long age = -1;
    int level = -1;
    uint64_t size = 0;
    {
      lock_guard<mutex> _(_sstMapLock);
      SstTemp* st = _sstMap[sst_id];
      temp = st->Temp(cur_time);
      age = st->Age(cur_time);
      level = st->Level();
      size = st->Size();
    }

    if (temp != TEMP_UNINITIALIZED)
      input_sst_temp.push_back(temp);

    input_sst_path_id.push_back(path_id);
    input_sst_info.push_back(str(boost::format("(sst_id=%d temp=%.3f level=%d path_id=%d size=%d age=%d)")
          % sst_id % temp % level % path_id % size % age));
  }

  // output_path_id starts from the min of input path_ids in case none of the temperatures is defined.
  uint32_t output_path_id = *std::min_element(input_sst_path_id.begin(), input_sst_path_id.end());

  // We use _sst_ott to decide the output path_id.
  {
    lock_guard<mutex> _(_sstOrgLock);
    if (_sst_ott != -1) {
      if (0 < input_sst_temp.size()) {
        double avg = std::accumulate(input_sst_temp.begin(), input_sst_temp.end(), 0.0) / input_sst_temp.size();
        if (_sst_ott < avg) {
          output_path_id = 0;
        } else {
          output_path_id = 1;
        }
      }
    }
  }

  // We don't change path_id when not wanted. Useful for measuring the CPU overhead of SSTable organizations.
  if (! _options.migrate_sstables)
    output_path_id = 0;

  JSONWriter jwriter;
  EventHelpers::AppendCurrentTime(&jwriter);
  jwriter << "mutant_sst_compaction_migration";
  jwriter.StartObject();
  if (0 < input_sst_info.size())
    jwriter << "in_sst" << boost::algorithm::join(input_sst_info, " ");
  jwriter << "out_sst_path_id" << output_path_id;
  jwriter << "temp_triggered_single_sst_compaction" << temperature_triggered_single_sstable_compaction;
  jwriter.EndObject();
  jwriter.EndObject();
  _logger->Log(jwriter);

  return output_path_id;
}


// Trivial move. db/db_impl.cc:3508
//   Used for single-SSTable migrations
uint32_t Mutant::_CalcOutputPathIdTrivialMove(const FileMetaData* fmd) {
  uint32_t path_id = fmd->fd.GetPathId();

  // Keep the current path_id when not initialized or migration is not wanted.
  if (! _initialized)
    return path_id;
  if (! _options.monitor_temp)
    return path_id;
  if (! _options.calc_sst_placement)
    return path_id;

  uint64_t sst_id = fmd->fd.GetNumber();
  uint32_t output_path_id = path_id;

  // We reuse _ssts_must_be_in_fast and _ssts_must_be_in_slow.
  //   They are unlikely to have been modified.
  {
    lock_guard<mutex> _(_sstOrgLock);

    if (_ssts_must_be_in_fast.count(sst_id) == 1) {
      output_path_id = 0;
    } else if (_ssts_must_be_in_slow.count(sst_id) == 1) {
      output_path_id = 1;
    } else {
      TRACE << boost::format("Interesting: sst_id=%d neither in _ssts_must_be_in_fast nor in _ssts_must_be_in_slow\n") % sst_id;
    }
  }

  // Keep the current path_id when migration is not wanted. Useful for measuring the compuration overhead.
  if (! _options.migrate_sstables)
    output_path_id = path_id;

  JSONWriter jwriter;
  EventHelpers::AppendCurrentTime(&jwriter);
  jwriter << "mutant_trivial_move";
  jwriter.StartObject();
  jwriter << "sst_id" << sst_id;
  jwriter << "level" << _SstLevel(sst_id);
  jwriter << "path_id" << path_id;
  jwriter << "temp" << _SstTemp(sst_id);
  jwriter << "output_path_id" << output_path_id;
  jwriter.EndObject();
  jwriter.EndObject();
  _logger->Log(jwriter);

  return output_path_id;
}


FileMetaData* Mutant::__GetSstFileMetaDataForMigration(const uint64_t sst_id, int& level_for_migration) {
  int filelevel;
  FileMetaData* fmd = nullptr;
  ColumnFamilyData* cfd;
  Status s = _db->MutantGetMetadataForFile(sst_id, &filelevel, &fmd, &cfd);
  if (s.code() == Status::kNotFound) {
    // This rarely happens, but happens. Ignore the sst.
    return nullptr;
  }
  if (!s.ok())
    THROW(boost::format("Unexpected: s=%s sst_id=%d") % s.ToString() % sst_id);
  if (!fmd)
    THROW(boost::format("Unexpected: s=%s sst_id=%d") % s.ToString() % sst_id);

  if (fmd->being_compacted)
    return nullptr;

  level_for_migration = filelevel;

  // There is no guarantee that the fmd points to a live SSTable after it is
  // returned. I think the sames goes with the regular compaction picking path.
  // Hope the compaction implementation takes care of it.
  return fmd;
}


// Pick an SSTable to migrate. Return nullptr when there is no SSTable for migration.
FileMetaData* Mutant::_PickSstToMigrate(int& level_for_migration) {
  if (! _initialized)
    return nullptr;
  if (! _options.monitor_temp)
    return nullptr;
  if (! _options.calc_sst_placement)
    return nullptr;
  if (! _db)
    return nullptr;

  {
    lock_guard<mutex> _1(_sstOrgLock);
    lock_guard<mutex> _(_sstMapLock);
    __SstOrgGreedyKnapsack(false);

    // Pick an SSTable to migrate
    //   (a) First, pick the hottest SSTable in the slow storage that needs to go to the fast storage
    //   (b) If none, pick the coldest SSTable in the fast storage that needs to go to the slow storage

    // (a)
    for (auto i = _ssts_must_be_in_fast_by_temp.rbegin(); i != _ssts_must_be_in_fast_by_temp.rend(); i ++) {
      uint64_t sst_id = i->second;
      SstTemp* st = _sstMap[sst_id];
      if (st->PathId() == 1) {
        FileMetaData* fmd = __GetSstFileMetaDataForMigration(sst_id, level_for_migration);
        if (fmd != nullptr) {
          if (! _options.migrate_sstables) {
            return nullptr;
          } else {
            return fmd;
          }
        }
      }
    }

    // (b)
    for (auto i = _ssts_must_be_in_slow_by_temp.begin(); i != _ssts_must_be_in_slow_by_temp.end(); i ++) {
      uint64_t sst_id = i->second;
      SstTemp* st = _sstMap[sst_id];
      if (st->PathId() == 0) {
        FileMetaData* fmd = __GetSstFileMetaDataForMigration(sst_id, level_for_migration);
        if (fmd != nullptr) {
          if (! _options.migrate_sstables) {
            return nullptr;
          } else {
            return fmd;
          }
        }
      }
    }
  }

  return nullptr;
}


void Mutant::_TempUpdaterRun() {
  try {
    boost::posix_time::ptime prev_time;

    while (! _temp_updater_stop_requested) {
      _TempUpdaterSleep();

      {
        lock_guard<mutex> lk(_temp_updating_mutex);

        boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

        // A 2-level check. _temp_updater_sleep_cv alone is not enough since this thread is woken up every second anyway.
        //
        // Print out the access stat. The entries are already sorted numerically.
        if (_updatedSinceLastOutput) {
          vector<string> memt_stat_str;
          {
            lock_guard<mutex> lk2(_memtSetLock);
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

          // map<path_id, multimap<temp, sst_status_str> >
          map<int, multimap<double, string> > pathid_temp_sststr;
          {

            lock_guard<mutex> lk2(_sstMapLock);
            for (auto i: _sstMap) {
              uint64_t sst_id = i.first;
              SstTemp* st = i.second;

              long c = st->GetAndResetNumReads();
              double dur_since_last_update;
              if (prev_time.is_not_a_date_time()) {
                // No need to worry about the time duration and the inaccuracy.  This happens only once right after RocksDB is
                // initialized.
                dur_since_last_update = _temp_update_interval_sec;
              } else {
                dur_since_last_update = (cur_time - prev_time).total_nanoseconds()
                  / 1000000000.0 / _simulation_time_dur_over_simulated;
              }
              double reads_per_64MB_per_sec = double(c) / (st->Size() / (64.0*1024*1024)) / dur_since_last_update;

              // Update temperature. You don't need to update st when c == 0.
              if (0 < c)
                st->UpdateTemp(c, reads_per_64MB_per_sec, cur_time);

              uint32_t path_id = st->PathId();
              double temp = st->Temp(cur_time);
              double age = st->Age(cur_time);
              string sst_sstr = str(boost::format("%.3f:%d:L%d:A%d:%d") % temp % sst_id % st->Level() % age % c);

              auto it = pathid_temp_sststr.find(path_id);
              if (it == pathid_temp_sststr.end()) {
                pathid_temp_sststr[path_id] = multimap<double, string>();
                pathid_temp_sststr[path_id].emplace(temp, sst_sstr);
              } else {
                it->second.emplace(temp, sst_sstr);
              }
            }
          }

          string ssts_in_fast;
          if (0 < pathid_temp_sststr.count(0)) {
            vector<string> s;
            for (auto it = pathid_temp_sststr[0].rbegin(); it != pathid_temp_sststr[0].rend(); it ++) {
              s.push_back(it->second);
            }
            ssts_in_fast = boost::algorithm::join(s, " ");
          }
          string ssts_in_slow;
          if (0 < pathid_temp_sststr.count(1)) {
            vector<string> s;
            for (auto it = pathid_temp_sststr[1].rbegin(); it != pathid_temp_sststr[1].rend(); it ++) {
              s.push_back(it->second);
            }
            ssts_in_slow = boost::algorithm::join(s, " ");
          }

          // Output to the rocksdb log. The condition is just to reduce
          // memt-only logs. So not all memt stats are reported, which is okay.
          if (0 < pathid_temp_sststr.size()) {
            JSONWriter jwriter;
            EventHelpers::AppendCurrentTime(&jwriter);
            jwriter << "mutant_table_acc_cnt";
            jwriter.StartObject();
            if (0 < memt_stat_str.size())
              jwriter << "memt" << boost::algorithm::join(memt_stat_str, " ");
            jwriter << "ssts_in_fast" << ssts_in_fast;
            jwriter << "ssts_in_slow" << ssts_in_slow;
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


// Sleep for _temp_update_interval_sec or until woken up by another thread.
void Mutant::_TempUpdaterSleep() {
  static const auto wait_dur = chrono::milliseconds(long(_temp_update_interval_sec * 1000.0 * _simulation_time_dur_over_simulated));
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


// A greedy knapsack based SSTable organization
void Mutant::__SstOrgGreedyKnapsack(bool log) {
  // Calc the total size of SSTables that can go to the fast storage.
  uint64_t total_sst_size = 0;
  for (auto i: _sstMap) {
    SstTemp* st = i.second;
    total_sst_size += st->Size();
  }

  // The conditions were checked in _Init()
  //   _options.stg_cost_list.size() == 2
  //   _options.stg_cost_list[1] <= _options.stg_cost_slo <= _options.stg_cost_list[0]

  double c0 = _options.stg_cost_list[0];
  double c1 = _options.stg_cost_list[1];
  double b = _options.stg_cost_slo;

  // c[1] : c[1] * S              -- (1) when you put all SSTables in the slow storage
  // b    : c[1] * Ss + c[0] * Sf -- (2)
  // c[0] : c[0] * S              --     when you put all SSTables in the fast storage
  //
  // S = Sf + Ss -- (3) Total SSTable size in fast and slow storages
  // Ss = S - Sf
  //
  // From (1) and (2),
  // c[1] * Ss + c[0] * Sf = c[1] * S * b / c[1] -- (4)
  //
  // From (3) and (4),
  // c[1] * S + (c[0] - c[1]) * Sf = c[1] * S * b / c[1]
  //
  // Sf = (c[1] * S * b / c[1] - c[1] * S) / (c[0] - c[1]) -- (5)
  //
  // To make the storage cost equal to or under the budget,
  // Sf <= (c[1] * S * b / c[1] - c[1] * S) / (c[0] - c[1]) -- (6)
  uint64_t total_sst_size_in_fast_max = (c1 * total_sst_size * b / c1 - c1 * total_sst_size) / (c0 - c1);

  // Classify SSTables into:
  //   - No action needed: Too young SSTables and SSTables with an uninitialized temperate
  //   - Must be migrated
  //   - Not optimal, but won't migrate
  //
  // (-) Level-based organization
  //   L0 SSTables. Optional (when _options.organize_L0_sstables = false).
  //   This helps when the database size is small.
  //     The temperature-based organization could migrate some L0 SSTables to the slow device.
  //     However, the read requests are most-likely be absorbed by the file system cache since there should be enough memory for caching all SSTables.
  //   I don't think this condition is necessary. Let's drop it.
  //
  // (a) Age-based organization
  //   The problem it solves:
  //     The temperature of a young SSTable can be unstable, causing undesirable migrations.
  //       A cold SSTable can be mistakenly thought of hot with just a couple of hits when it's created.
  //       A hot SSTable can be thought of cold because there was no hit when it's created.
  //         This is not very likely to happen, but still could happen.
  //   Solution:
  //     Too young, currently-in-fast-storage SSTables stay in the fast storage. sst_in_fast_by_age
  //     Too young, currently-in-slow-storage SSTables stay in the slow storage. sst_in_slow_by_age
  //
  //   The solution can violate the cost SLO when there are not enough SSTables.
  //     However, the violation is only when the data size is very small.
  //
  //   This can be alternatively implemented by not defining the SSTable temperature when they are too young.
  //
  // (b) Temperature-based organization
  //   (b.1) SSTables with an uninitialized temp go to either sst_in_fast_temp_uninit or sst_in_slow_temp_uninit
  //   (b.2)
  //     While it satisfies the cost SLO
  //       Currently the hottest SSTable goes to the fast storage
  //       Put it in sst_in_fast_by_temp
  //     Put the others in sst_in_slow_by_temp
  //   (b.3) Calc and keep SSTable organization temperature threshold (_sst_ott) for deciding where the future SSTables should go based on their temperatures
  //     Note that sst_ott can only be defined with the greedy algorithm.
  //       Not with an optimal solution with the dynamic algorithm, since there may not be a single boundary between hot and cold.
  //
  // (c) Hysteresis
  //   Mark the coldest half-epsilson amount of SSTables in sst_in_fast_by_temp as dont-migrate; and the others as must-migrate.
  //   Do the same with the SSTables in sst_in_slow_by_temp.
  //
  //   The hysteresis is a trade-off of performance (throughput and latency) vs. the number of back-and-forth SSTable migrations.
  //     In fact, I think the performance implication would be miniscule.
  //       From what I observed with YCSB D and 0.4, there weren't much difference in the hit counts near the boundary.
  //     Cost SLO is always met; the hysteresis doesn't affect cost..

  // (a)
  set<uint64_t> sst_in_fast_by_age;
  set<uint64_t> sst_in_slow_by_age;
  uint64_t sum_sst_sizes_in_fast = 0;
  uint64_t sum_sst_sizes_in_slow = 0;
  set<uint64_t> ssts_taken;
  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();
  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    SstTemp* st = i.second;
    uint64_t size = st->Size();
    uint32_t path_id = st->PathId();
    if (st->Age(cur_time) < YOUNG_SST_AGE_CUTOFF) {
      if (path_id == 0) {
        sst_in_fast_by_age.insert(sst_id);
        sum_sst_sizes_in_fast += size;
        ssts_taken.insert(sst_id);
      } else if (path_id == 1) {
        sst_in_slow_by_age.insert(sst_id);
        sum_sst_sizes_in_slow += size;
        ssts_taken.insert(sst_id);
      } else {
        THROW(boost::format("Unexpected: %d") % path_id);
      }
    }
  }

  // (b.1)
  set<uint64_t> ssts_in_fast_by_temp_uninit;
  set<uint64_t> ssts_in_slow_by_temp_uninit;
  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    if (ssts_taken.count(sst_id) == 1)
      continue;

    SstTemp* st = i.second;
    double temp = st->Temp(cur_time);
    uint32_t path_id = st->PathId();
    uint64_t size = st->Size();
    if (temp == TEMP_UNINITIALIZED) {
      if (path_id == 0) {
        ssts_in_fast_by_temp_uninit.insert(sst_id);
        sum_sst_sizes_in_fast += size;
        ssts_taken.insert(sst_id);
      } else if (path_id == 1) {
        ssts_in_slow_by_temp_uninit.insert(sst_id);
        sum_sst_sizes_in_slow += size;
        ssts_taken.insert(sst_id);
      } else {
        THROW(boost::format("Unexpected: %d") % path_id);
      }
    }
  }

  // A cost SLO violation can happen because of the above conditions: (a) and (b.1).
  //   This can happen
  //     when there is only a small number of SSTables.
  //     when you recover a DB that wasn't properly organized by a cost SLO.
  bool met_cost_slo = true;
  if (total_sst_size_in_fast_max < sum_sst_sizes_in_fast) {
    met_cost_slo = false;
  }

  // (b.2)
  multimap<double, uint64_t> temp_sstid;
  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    if (ssts_taken.count(sst_id) == 1)
      continue;
    SstTemp* st = i.second;
    double t = st->Temp(cur_time);
    temp_sstid.emplace(t, sst_id);
  }
  multimap<double, uint64_t> ssts_in_fast_by_temp;
  multimap<double, uint64_t> ssts_in_slow_by_temp;
  double coldest_temp_in_fast = -1;
  double hottest_temp_in_slow = -1;
  for (auto i = temp_sstid.rbegin(); i != temp_sstid.rend(); i ++) {
    double temp = i->first;
    uint64_t sst_id = i->second;
    SstTemp* st = _sstMap[sst_id];
    uint64_t sst_size = st->Size();

    // Add the next hottest SSTable to the fast storage if there is space. If not, add the SSTable to the slow storage.
    if (sum_sst_sizes_in_fast + sst_size <= total_sst_size_in_fast_max) {
      ssts_in_fast_by_temp.emplace(temp, sst_id);
      sum_sst_sizes_in_fast += sst_size;
      coldest_temp_in_fast = temp;
    } else {
      ssts_in_slow_by_temp.emplace(temp, sst_id);
      sum_sst_sizes_in_fast += sst_size;
      if (hottest_temp_in_slow == -1)
        hottest_temp_in_slow = temp;
    }
  }

  // (b.3)
  if (coldest_temp_in_fast == -1) {
    if (hottest_temp_in_slow == -1) {
      // Undefined
      _sst_ott = -1;
    } else {
      _sst_ott = hottest_temp_in_slow;
    }
  } else {
    if (hottest_temp_in_slow == -1) {
      _sst_ott = coldest_temp_in_fast;
    } else {
      _sst_ott = (coldest_temp_in_fast + hottest_temp_in_slow) / 2.0;
    }
  }

  // (c)
  uint64_t half_sst_size_in_hysteresis_range = total_sst_size * _options.stg_cost_slo_epsilon / 2.0;
  set<uint64_t> ssts_in_fast_by_temp_no_migr;
  set<uint64_t> ssts_in_slow_by_temp_no_migr;
  _ssts_must_be_in_fast.clear();
  _ssts_must_be_in_fast_by_temp.clear();
  uint64_t s = 0;
  for (auto i = ssts_in_fast_by_temp.begin(); i != ssts_in_fast_by_temp.end(); i ++) {
    double temp = i->first;
    uint64_t sst_id = i->second;
    uint64_t size = _sstMap[sst_id]->Size();
    if (s + size <= half_sst_size_in_hysteresis_range) {
      ssts_in_fast_by_temp_no_migr.insert(sst_id);
      s += size;
    } else {
      _ssts_must_be_in_fast.insert(sst_id);
      _ssts_must_be_in_fast_by_temp.emplace(temp, sst_id);
    }
  }
  _ssts_must_be_in_slow.clear();
  _ssts_must_be_in_slow_by_temp.clear();
  s = 0;
  for (auto i = ssts_in_slow_by_temp.rbegin(); i != ssts_in_slow_by_temp.rend(); i ++) {
    double temp = i->first;
    uint64_t sst_id = i->second;
    uint64_t size = _sstMap[sst_id]->Size();
    if (s + size <= half_sst_size_in_hysteresis_range) {
      ssts_in_slow_by_temp_no_migr.insert(sst_id);
      s += size;
    } else {
      _ssts_must_be_in_slow.insert(sst_id);
      _ssts_must_be_in_slow_by_temp.emplace(temp, sst_id);
    }
  }

  if (log) {
    size_t ssts_in_fast = sst_in_fast_by_age.size() + ssts_in_fast_by_temp_uninit.size() + ssts_in_fast_by_temp.size();
    string ssts_in_fast_str = str(boost::format("num=%d size=%d num_too_young=%d num_temp_uninit=%d num_by_temp_must=%d num_by_temp_no_migr=%d")
        % ssts_in_fast % sum_sst_sizes_in_fast % sst_in_fast_by_age.size() % ssts_in_fast_by_temp_uninit.size()
        % _ssts_must_be_in_fast.size()
        % ssts_in_fast_by_temp_no_migr.size()
        );
    size_t ssts_in_slow = sst_in_slow_by_age.size() + ssts_in_slow_by_temp_uninit.size() + ssts_in_slow_by_temp.size();
    string ssts_in_slow_str = str(boost::format("num=%d size=%d num_too_young=%d num_temp_uninit=%d num_by_temp_must=%d num_by_temp_no_migr=%d")
        % ssts_in_slow % sum_sst_sizes_in_slow % sst_in_slow_by_age.size() % ssts_in_slow_by_temp_uninit.size()
        % _ssts_must_be_in_slow.size()
        % ssts_in_slow_by_temp_no_migr.size()
        );

    JSONWriter jwriter;
    EventHelpers::AppendCurrentTime(&jwriter);
    jwriter << "mutant_sst_org";
    jwriter.StartObject();
    jwriter << "total_sst_size" << total_sst_size;
    jwriter << "total_sst_size_in_fast_max" << total_sst_size_in_fast_max;
    jwriter << "met_cost_slo" << met_cost_slo;
    jwriter << "ssts_in_fast" << ssts_in_fast_str;
    jwriter << "ssts_in_slow" << ssts_in_slow_str;
    jwriter << "sst_ott" << _sst_ott;
    jwriter.EndObject();
    jwriter.EndObject();
    _logger->Log(jwriter);
  }
}


// Schedule a background SSTable compaction when an SSTable needs to be migrated.
//
// When you schedule a background compaction and there is no compaction to do, you get a "Compaction nothing to do".
//   Though seems harmless, you don't want unnecessarily scheduling of compactions.
void Mutant::_SstMigrationTriggererRun() {
  try {
    while (! _smt_stop_requested) {
      _SstMigrationTriggererSleep();

      if (!_cfd)
        continue;

      // Regular compactions takes priority over temperature-triggered compactions.
      if (0 < _db->UnscheduledCompactions())
        continue;

      bool may_have_sstable_to_migrate = false;

      {
        lock_guard<mutex> _1(_sstOrgLock);
        {
          lock_guard<mutex> _(_sstMapLock);
          __SstOrgGreedyKnapsack(true);

          // Do we perhaps need to migrate an SSTable?
          //   Schedule a compaction (migration) only when there is an SSTable that needs to be migrated.
          //     This is an initial check. _PickSstToMigrate() does another check later,
          //       because it can change between this function (triggerer that schedules a compaction) and LevelCompactionPicker::PickCompaction().
          for (auto i: _sstMap) {
            uint64_t sst_id = i.first;
            uint32_t path_id = i.second->PathId();
            if (path_id == 1 && _ssts_must_be_in_fast.count(sst_id) == 1) {
              may_have_sstable_to_migrate = true;
              break;
            }
            if (path_id == 0 && _ssts_must_be_in_slow.count(sst_id) == 1) {
              may_have_sstable_to_migrate = true;
              break;
            }
          }
        }
      }

      JSONWriter jwriter;
      EventHelpers::AppendCurrentTime(&jwriter);
      jwriter << "mutant_may_have_sstable_to_migrate" << may_have_sstable_to_migrate;
      jwriter.EndObject();
      _logger->Log(jwriter);

      if (may_have_sstable_to_migrate)
        _db->MutantMayScheduleCompaction(_cfd);
    }
  } catch (const exception& e) {
    TRACE << boost::format("Exception: %s\n") % e.what();
    exit(0);
  }
}


// These 2 functions are not called when monitor_temp = false
void Mutant::_SstMigrationTriggererSleep() {
  static const auto wait_dur = chrono::milliseconds(long(_sst_migration_triggerer_interval_sec * 1000.0 * _simulation_time_dur_over_simulated));
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


void Mutant::_SetNumRunningCompactions(int n) {
  if (! _initialized)
    return;

  lock_guard<mutex> _(_last_sst_write_time_lock);
  _num_running_compactions = n;
  if (0 < _num_running_compactions + _num_running_flushes) {
    // Assign not-a-date-time
    _last_sst_write_time = boost::posix_time::ptime();
  } else if (0 == _num_running_compactions + _num_running_flushes) {
    _last_sst_write_time = boost::posix_time::microsec_clock::local_time();
  }
}


void Mutant::_SetNumRunningFlushes(int n) {
  if (! _initialized)
    return;

  lock_guard<mutex> _(_last_sst_write_time_lock);
  _num_running_flushes = n;
  if (0 < _num_running_compactions + _num_running_flushes) {
    // Assign not-a-date-time
    _last_sst_write_time = boost::posix_time::ptime();
  } else if (0 == _num_running_compactions + _num_running_flushes) {
    _last_sst_write_time = boost::posix_time::microsec_clock::local_time();
  }
}


void Mutant::_Shutdown() {
  if (! _initialized)
    return;

  static mutex m;
  lock_guard<mutex> _(m);

  _smt_stop_requested = true;
  _SstMigrationTriggererWakeup();
  if (_smt_thread) {
    _smt_thread->join();
    delete _smt_thread;
    _smt_thread = nullptr;
  }

  {
    lock_guard<mutex> _2(_temp_updating_mutex);
    _temp_updater_stop_requested = true;
    _TempUpdaterWakeup();
  }
  if (_temp_updater_thread) {
    _temp_updater_thread->join();
    delete _temp_updater_thread;
    _temp_updater_thread = nullptr;
  }
}


const DBOptions::MutantOptions* Mutant::_Options() {
  if (_initialized)
    return &_options;
  else
    return nullptr;
}


void Mutant::Init(const DBOptions::MutantOptions* mo, DBImpl* db, EventLogger* el) {
  static Mutant& i = _GetInst();
  i._Init(mo, db, el);
}


void Mutant::MemtCreated(ColumnFamilyData* cfd, MemTable* m) {
  // This is called more often than SstOpened() at least in the beginning of
  // the experiment. I think it won't be less often even when the DB restarts.
  //
  //TRACE << boost::format("%d cfd=%p cfd_name=%s\n") % std::this_thread::get_id() % cfd % cfd->GetName();

  const auto cfd_name = cfd->GetName();
  if (cfd_name != "default" && cfd_name != "usertable") {
    // Just checking if there are any other CFs other than the user-provided ones cause Cassandra has system CFs.
    //   quizup uses default. YCSB uses usertable.
    TRACE << boost::format("%d Interesting! cfd=%p cfd_name=%s\n") % std::this_thread::get_id() % cfd % cfd->GetName();
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
    const std::vector<FileMetaData*>& file_metadata,
    int output_level) {
  static Mutant& i = _GetInst();
  return i._CalcOutputPathId(temperature_triggered_single_sstable_compaction, file_metadata, output_level);
}


uint32_t Mutant::CalcOutputPathIdTrivialMove(const FileMetaData* fmd) {
  static Mutant& i = _GetInst();
  return i._CalcOutputPathIdTrivialMove(fmd);
}


FileMetaData* Mutant::PickSstToMigrate(int& level_for_migration) {
  static Mutant& i = _GetInst();
  return i._PickSstToMigrate(level_for_migration);
}


void Mutant::SetNumRunningCompactions(int n) {
  static Mutant& i = _GetInst();
  i._SetNumRunningCompactions(n);
}


void Mutant::SetNumRunningFlushes(int n) {
  static Mutant& i = _GetInst();
  i._SetNumRunningFlushes(n);
}


void Mutant::Shutdown() {
  static Mutant& i = _GetInst();
  i._Shutdown();
}


const DBOptions::MutantOptions* Mutant::Options() {
  static Mutant& i = _GetInst();
  return i._Options();
}

}
