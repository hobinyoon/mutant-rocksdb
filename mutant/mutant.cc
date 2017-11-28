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


// https://en.wikipedia.org/wiki/PID_controller
//class SlaAdmin {
//  double _target_value;
//  double _p;
//  // Since the integral term responds to accumulated errors from the past, it can cause the present value to overshoot the setpoint value
//  double _i;
//  // Derivative action predicts system behavior and thus improves settling time and stability of the system.
//  // Derivative action is seldom used in practice though – by one estimate in only 25% of deployed controllers – because of its variable
//  // impact on system stability in real-world applications.
//  double _d;
//
//  double _prev_error = 0.0;
//  double _integral = 0.0;
//
//  boost::posix_time::ptime _prev_ts;
//  bool _prev_ts_defined = false;
//
//  const DBOptions::MutantOptions& mu_options;
//  EventLogger* _logger;
//
//public:
//  SlaAdmin(double target_value, double p, double i, double d, const DBOptions::MutantOptions& options_, EventLogger* logger)
//    : _target_value(target_value), _p(p), _i(i), _d(d), mu_options(options_)
//  {
//    if (logger == nullptr)
//      THROW("Unexpected");
//    _logger = logger;
//
//    {
//      JSONWriter jwriter;
//      EventHelpers::AppendCurrentTime(&jwriter);
//      jwriter << "mutant_sla_admin_init";
//      jwriter.StartObject();
//      jwriter << "target_value" << target_value;
//      jwriter << "p" << _p;
//      jwriter << "i" << _i;
//      jwriter << "d" << _d;
//      jwriter.EndObject();
//      jwriter.EndObject();
//      _logger->Log(jwriter);
//    }
//  }
//
//  virtual ~SlaAdmin() { }
//
//  // Calc an adjustment to the controlling value.
//  //   The output of the PID controller should be an adjustment to the control variable. Not a direct value of the variable.
//  //     E.g., when there is no error, the sst_ott can be like 10.
//  double CalcAdj(double cur_value, JSONWriter& jwriter) {
//    // Prevent suddern peaks lowering sst_ott too fast. We don't need this now.
//    //if (_target_value * 2.0 < cur_value)
//    //  cur_value = _target_value * 2.0;
//
//    double error = _target_value - cur_value;
//
//    boost::posix_time::ptime ts = boost::posix_time::microsec_clock::local_time();
//
//    // No integral or derivative term on the first fun
//    double derivative = 0.0;
//    double dt = 0.0;
//    if (_prev_ts_defined) {
//      dt = (ts - _prev_ts).total_milliseconds() / 1000.0;
//      // Apply an exponential decay to I
//      _integral *= pow(mu_options.pid_i_exp_decay_factor, dt);
//      _integral += (error * dt);
//      if (dt != 0.0) {
//        derivative = (error - _prev_error) / dt;
//      }
//    }
//
//    _prev_error = error;
//    _prev_ts = ts;
//    _prev_ts_defined = true;
//
//    double adj = _p * error + _i * _integral + _d * derivative;
//
//    // Note: enclose this with something like cur_pid_values
//    jwriter << "dt" << dt;
//    jwriter << "p" << error;
//    jwriter << "i" << _integral;
//    jwriter << "d" << derivative;
//    jwriter << "adj" << adj;
//
//    // Relative adjustment value to _target_value
//    double adj1 = adj / _target_value;
//    jwriter << "adj1" << adj1;
//
//    return adj1;
//  }
//};


//class DiskMon {
//private:
//  string _fn;
//  double _prev_ts = -1;
//  long _prev_read_ios = 0;
//  long _prev_write_ios = 0;
//
//public:
//  DiskMon(const string& dev) {
//    _fn = str(boost::format("/sys/block/%s/stat") % dev);
//  }
//
//  // Get read and write iops. -1 when undefined.
//  void Get(double& r, double& w) {
//    // https://www.kernel.org/doc/Documentation/block/stat.txt
//    //
//    // Name            units         description
//    // ----            -----         -----------
//    // read I/Os       requests      number of read I/Os processed
//    // read merges     requests      number of read I/Os merged with in-queue I/O
//    // read sectors    sectors       number of sectors read
//    // read ticks      milliseconds  total wait time for read requests
//    // write I/Os      requests      number of write I/Os processed
//    // write merges    requests      number of write I/Os merged with in-queue I/O
//    // write sectors   sectors       number of sectors written
//    // write ticks     milliseconds  total wait time for write requests
//    // in_flight       requests      number of I/Os currently in flight
//    // io_ticks        milliseconds  total time this block device has been active
//    // time_in_queue   milliseconds  total wait time for all requests
//    //
//    // $ cat /sys/block/xvde/stat
//    // 144487        0  2303026   233216   428849 11938402 108265496 12622636        0   464196 12855836
//    string line;
//    {
//      ifstream ifs(_fn);
//      if (! getline(ifs, line))
//        THROW("Unexpected");
//    }
//    boost::trim(line);
//    static const auto sep = boost::is_any_of(" ");
//    vector<string> t;
//    boost::split(t, line, sep, boost::algorithm::token_compress_on);
//    if (t.size() != 11)
//      THROW(boost::format("Unexpected. %d [%s]\n") % t.size() % line);
//    long read_ios = atol(t[0].c_str());
//    long write_ios = atol(t[4].c_str());
//
//    timeval tv;
//    gettimeofday(&tv, 0);
//    double ts_now = tv.tv_sec + tv.tv_usec * 0.000001;
//
//    if (_prev_ts == -1) {
//      r = -1;
//      w = -1;
//    } else {
//      long r_delta = read_ios - _prev_read_ios;
//      long w_delta = write_ios - _prev_write_ios;
//      double ts_delta = ts_now - _prev_ts;
//      if (ts_delta == 0.0) {
//        r = -1;
//        w = -1;
//      } else {
//        r = r_delta / ts_delta;
//        w = w_delta / ts_delta;
//      }
//    }
//
//    _prev_ts = ts_now;
//    _prev_read_ios = read_ios;
//    _prev_write_ios = write_ios;
//  }
//};


Mutant& Mutant::_GetInst() {
  // The object instance won't be released since it's static, singleton.
  static Mutant i;
  return i;
}


Mutant::Mutant()
{
}


// TODO: remove all sst_ott


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

  // _smt_thread is needed only with migrate_sstables
  if (_options.migrate_sstables) {
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
    TRACE << cfd->GetName() << "\n";
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
  if (! _initialized) {
    TRACE << "Interesting...\n";
    return;
  }
  if (! _options.monitor_temp)
    return;

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
  if (! _initialized)
    return 0;
  if (! _options.monitor_temp)
    return 0;
  if (! _options.migrate_sstables)
    return 0;
  if ((!_options.organize_L0_sstables) && output_level <= 0)
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
    double temp = _SstTemperature(sst_id, cur_time);

    if (temp != TEMP_UNINITIALIZED)
      input_sst_temp.push_back(temp);

    input_sst_path_id.push_back(path_id);

    input_sst_info.push_back(str(boost::format("(sst_id=%d level=%d path_id=%d temp=%.3f)")
          % sst_id % _SstLevel(sst_id) % path_id % temp));
  }

  // output_path_id starts from the min of input path_ids in case none of the temperatures is defined.
  uint32_t output_path_id = *std::min_element(input_sst_path_id.begin(), input_sst_path_id.end());

  // This is a trickly one. How do you know if the average is hot or cold without sst_ott (organization temperature threshold)?
  //   Options:
  //     Use the average of the weighted value of the binary value (hot or cold).
  //     Calculate the threshold when doing the knapsack based SSTable organization. This.
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
//   Used for single SSTable migrations
uint32_t Mutant::_CalcOutputPathIdTrivialMove(const FileMetaData* fmd) {
  uint32_t path_id = fmd->fd.GetPathId();

  if (! _initialized)
    return path_id;
  if (! _options.monitor_temp)
    return path_id;
  if (! _options.migrate_sstables)
    return path_id;

  uint64_t sst_id = fmd->fd.GetNumber();
  uint32_t output_path_id = path_id;

  // We reuse _ssts_in_fast and _ssts_in_slow.
  //   They are unlikely to have been modified.
  {
    lock_guard<mutex> _(_sstOrgLock);

    if (_ssts_in_fast.count(sst_id) == 1) {
      output_path_id = 0;
    } else if (_ssts_in_slow.count(sst_id) == 1) {
      output_path_id = 1;
    } else {
      TRACE << boost::format("Interesting: sst_id=%d neither in _ssts_in_fast nor in _ssts_in_slow\n") % sst_id;
    }
  }

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


// Pick an SSTable to migrate.
//   When moving an SSTable to fast dev, pick the hottest one over sst_ott.
//   When moving an SSTable to slow dev, pick the coldest one over sst_ott.
// Returns nullptr when there is no SSTable for migration
FileMetaData* Mutant::_PickSstToMigrate(int& level_for_migration) {
  if (! _initialized)
    return nullptr;
  if (! _options.monitor_temp)
    return nullptr;
  if (! _options.migrate_sstables)
    return nullptr;
  if (! _db)
    return nullptr;

  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();

  {
    lock_guard<mutex> _1(_sstOrgLock);
    _ssts_in_fast.clear();
    _ssts_in_slow.clear();

    lock_guard<mutex> _(_sstMapLock);
    __SstOrgGreedyKnapsack(cur_time, false);

    // Pick an SSTable to migrate
    //   (a) First, pick a hottest SSTable in the slow storage that needs to go to the fast storage
    //   (b) If none, pick a coldest SSTable in the fast storage that needs to go to the slow storage

    // (a)
    {
      multimap<double, uint64_t> temp_sstid;
      for (uint64_t sst_id: _ssts_in_fast) {
        SstTemp* st = _sstMap[sst_id];
        if (st->PathId() == 1) {
          double temp = st->Temp(cur_time);
          if (temp != TEMP_UNINITIALIZED)
            temp_sstid.emplace(temp, sst_id);
        }
      }
      for (auto i = temp_sstid.rbegin(); i != temp_sstid.rend(); i ++) {
        uint64_t sst_id = i->second;
        FileMetaData* fmd = __GetSstFileMetaDataForMigration(sst_id, level_for_migration);
        if (fmd != nullptr)
          return fmd;
      }
    }

    // (b)
    {
      multimap<double, uint64_t> temp_sstid;
      for (uint64_t sst_id: _ssts_in_slow) {
        SstTemp* st = _sstMap[sst_id];
        if (st->PathId() == 0) {
          double temp = st->Temp(cur_time);
          if (temp != TEMP_UNINITIALIZED)
            temp_sstid.emplace(temp, sst_id);
        }
      }
      for (auto i = temp_sstid.begin(); i != temp_sstid.end(); i ++) {
        uint64_t sst_id = i->second;
        FileMetaData* fmd = __GetSstFileMetaDataForMigration(sst_id, level_for_migration);
        if (fmd != nullptr)
          return fmd;
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

          vector<string> sst_status_str;
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

              sst_status_str.push_back(str(boost::format("%d:%d:%d:%.3f:%.3f")
                    % sst_id % st->Level() % c % reads_per_64MB_per_sec % st->Temp(cur_time)));
            }
          }

          // Output to the rocksdb log. The condition is just to reduce
          // memt-only logs. So not all memt stats are reported, which is okay.
          if (sst_status_str.size() > 0) {
            JSONWriter jwriter;
            EventHelpers::AppendCurrentTime(&jwriter);
            jwriter << "mutant_table_acc_cnt";
            jwriter.StartObject();
            if (memt_stat_str.size() > 0)
              jwriter << "memt" << boost::algorithm::join(memt_stat_str, " ");
            if (sst_status_str.size() > 0)
              jwriter << "sst" << boost::algorithm::join(sst_status_str, " ");
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
void Mutant::__SstOrgGreedyKnapsack(const boost::posix_time::ptime& cur_time, bool log) {
  uint64_t total_sst_size = 0;
  uint64_t total_sst_size_in_fast_max = 0;
  uint64_t cur_sst_size_in_fast = 0;
  uint64_t cur_sst_size_in_slow = 0;
  bool met_cost_slo = true;

  // Calc the total size of SSTables that can go to the fast storage.
  for (auto i: _sstMap) {
    SstTemp* st = i.second;
    total_sst_size += st->Size();
  }

  // The conditions were checked before
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
  total_sst_size_in_fast_max = (c1 * total_sst_size * b / c1 - c1 * total_sst_size) / (c0 - c1);

  // Group SSTables into fast and slow storage; Assign SSTables that go to the fast storage.
  //   (a) Put L0 SSTables, if _options.organize_L0_sstables = true.
  //   (b) Put young, currently in fast storage SSTables to the list.
  //   (c) Keep the SSTables with uninitialized temperature where they are
  //   (d) Put hot temperature SSTables.
  //     While skipping young, currently in slow storage SSTables.

  // (a)
  if (! _options.organize_L0_sstables) {
    for (auto i: _sstMap) {
      SstTemp* st = i.second;
      if (st->Level() == 0) {
        uint64_t sst_id = i.first;
        uint64_t sst_size = st->Size();
        _ssts_in_fast.insert(sst_id);
        cur_sst_size_in_fast += sst_size;
      }
    }
  }

  // (b)
  // The not migrate too young SSTables rule
  //   What problem does it solve?
  //     Frequent back-and-forth SSTable migrations.
  //     New SSTables from a Memtable flush. They are L0 SSTables that are to be compacted away soon.
  //
  //   When an SSTable is too young, such as younger than 30 secs,
  //     if the current path_id == 0, keep it in the fast storage
  //       Put it in the fast storage SSTable list before any SSTables
  //     if the current path_id == 1, keep it in the slow storage
  //       Do not put it in the fast storage SSTable list.
  //
  // TODO: How do you compare the logic with hysterisis?
  //   The first thought was ignoring SSTables in the boundary temperatature range from organization.
  //     You need to define the range, and keep track of how long an SSTable has been in the range.
  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    SstTemp* st = i.second;
    uint64_t sst_size = st->Size();
    if ((st->Age(cur_time) < YOUNG_SST_AGE_CUTOFF) && (st->PathId() == 0)) {
      _ssts_in_fast.insert(sst_id);
      cur_sst_size_in_fast += sst_size;
    }
  }

  // (c) Keep the SSTables with uninitialized temperature to where they are.
  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    SstTemp* st = i.second;
    if (st->Temp(cur_time) == TEMP_UNINITIALIZED && (st->PathId() == 0)) {
      _ssts_in_fast.insert(sst_id);
      cur_sst_size_in_fast += st->Size();
    }
  }

  // A cost SLO violation can happen because of the first two conditions.
  //   When only a small number of SSTables, an SLO can be hard to be met.
  //     With (a), it becomes harder.
  //   When there are a lot of young SSTables, which happens when you recover a DB and you don't know when they were actually created,
  //     a lot of SSTables go to the fast storage, causing a violation.
  //     This is kind of an implementation flaw. You should be able to know when they were created.
  //       Couldn't find one from a quick scan of BlockBasedTable.
  //       You may have to change the SSTable format, or store the information somewhere else.
  if (total_sst_size_in_fast_max < cur_sst_size_in_fast) {
    met_cost_slo = false;
  }

  // (d) Add hot SSTables to _ssts_in_fast
  multimap<double, uint64_t> temp_sstid;
  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    SstTemp* st = i.second;
    double t = st->Temp(cur_time);
    temp_sstid.emplace(t, sst_id);
  }
  for (auto i = temp_sstid.rbegin(); i != temp_sstid.rend(); i ++) {
    uint64_t sst_id = i->second;
    // Skip if it's already included
    if (_ssts_in_fast.count(sst_id) == 1)
      continue;

    // Skip if it's too young and currently in the slow storage
    SstTemp* st = _sstMap[sst_id];
    if ((st->Age(cur_time) < YOUNG_SST_AGE_CUTOFF) && (st->PathId() == 1))
      continue;

    // SSTables with uninitialized temp that are in slow storage should stay there.
    if (st->Temp(cur_time) == TEMP_UNINITIALIZED && (st->PathId() == 1))
      continue;

    // Add the next hottest SSTable to the fast storage when there is space
    uint64_t sst_size = st->Size();
    if (total_sst_size_in_fast_max < cur_sst_size_in_fast + sst_size) {
      // SSTable organization temperature threshold. This is defined only with the greedy algorithm.
      //   Not with an optimal solution with the dynamic algorithm, since there may not be a single boundary between hot and cold.
      double temp_i = i->first;
      auto ip1 = i;
      ip1 ++;
      if (ip1 == temp_sstid.rend()) {
        _sst_ott = temp_i;
      } else {
        double temp_ip1 = ip1->first;
        _sst_ott = (temp_i + temp_ip1) / 2.0;
      }

      break;
    }
    _ssts_in_fast.insert(sst_id);
    cur_sst_size_in_fast += sst_size;
  }

  for (auto i: _sstMap) {
    uint64_t sst_id = i.first;
    if (_ssts_in_fast.count(sst_id) == 0) {
      _ssts_in_slow.insert(sst_id);
      uint64_t sst_size = i.second->Size();
      cur_sst_size_in_slow += sst_size;
    }
  }

  if (log) {
    string ssts_in_fast_str = str(boost::format("(%d %d) %s")
        % _ssts_in_fast.size()
        % cur_sst_size_in_fast
        % boost::algorithm::join(_ssts_in_fast | boost::adaptors::transformed([](uint64_t i) { return std::to_string(i); }), " "));
    string ssts_in_slow_str = str(boost::format("(%d %d) %s")
        % _ssts_in_slow.size()
        % cur_sst_size_in_slow
        % boost::algorithm::join(_ssts_in_slow | boost::adaptors::transformed([](uint64_t i) { return std::to_string(i); }), " "));

    JSONWriter jwriter;
    EventHelpers::AppendCurrentTime(&jwriter);
    jwriter << "mutant_sst_org";
    jwriter.StartObject();
    jwriter << "total_sst_size" << total_sst_size;
    jwriter << "total_sst_size_in_fast_max" << total_sst_size_in_fast_max;
    jwriter << "met_cost_slo" << met_cost_slo;
    jwriter << "ssts_in_fast" << ssts_in_fast_str;
    jwriter << "ssts_in_slow" << ssts_in_slow_str;
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
      if (_db->UnscheduledCompactions() > 0)
        continue;

      boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();
      bool may_have_sstable_to_migrate = false;

      {
        lock_guard<mutex> _1(_sstOrgLock);
        _ssts_in_fast.clear();
        _ssts_in_slow.clear();

        {
          lock_guard<mutex> _(_sstMapLock);
          __SstOrgGreedyKnapsack(cur_time, true);

          // Do we perhaps have an SSTable for migration?
          //   Schedule a compaction (migration) only when there is an SSTable that needs to be migrated.
          //     This is an initial check. _PickSstToMigrate() does another check later,
          //       because it can change between this function (triggerer that schedules a compaction) and LevelCompactionPicker::PickCompaction().
          for (auto i: _sstMap) {
            uint64_t sst_id = i.first;
            uint32_t path_id = i.second->PathId();
            if ((_ssts_in_fast.count(sst_id) == 1) && (path_id == 1)) {
              may_have_sstable_to_migrate = true;
              break;
            }
            if ((_ssts_in_slow.count(sst_id) == 1) && (path_id == 0)) {
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


//void Mutant::_SlaAdminInit(double target_lat, double p, double i, double d) {
//  static mutex m;
//  lock_guard<mutex> _(m);
//  if (_sla_admin == nullptr) {
//    _sla_admin = new SlaAdmin(target_lat, p, i, d, _options, _logger);
//    _disk_mon = new DiskMon(_options.slow_dev);
//  }
//}


//void Mutant::_SlaAdminAdjust(double lat) {
//  if (_sla_admin == nullptr)
//    THROW("Unexpected");
//
//  bool make_adjustment = true;
//  double slow_dev_r_iops;
//  double slow_dev_w_iops;
//  _disk_mon->Get(slow_dev_r_iops, slow_dev_w_iops);
//
//  // TODO: hope we don't need this thanks to the PID controller.
//  if (false) {
//    if (_options.sla_admin_type == "latency") {
//      // No adjustment when the latency spikes by more than 3.0x.
//      //   Only when there is enough latency data in _lat_hist
//      //   This is to filter out high latencies that were not filtered out by the above test.
//      //     The inaccuracy is caused from the sporadic, client-measured adjustments.
//      //     Better solution would be measuring the latency inside the DB and making sure they were not affected by a compaction or a flush.
//      //       Still won't be easy to be perfect.
//      lock_guard<mutex> _(_lat_hist_lock);
//      if (_options.sla_observed_value_hist_q_size <= _lat_hist.size()) {
//        double running_avg = std::accumulate(_lat_hist.begin(), _lat_hist.end(), 0.0) / _lat_hist.size();
//        if (running_avg * 3.0 < lat) {
//          make_adjustment = false;
//        }
//      }
//    }
//  }
//
//  if (_options.sla_admin_type == "slow_dev_r_iops") {
//    // Filter out transient high read IOs probably caused by SSTable compactions / migrations.
//    //   3TB st1 seems to saturate around 550 iops. Anything above that indicates a transient boost.
//    if (600 < slow_dev_r_iops)
//      make_adjustment = false;
//  }
//
//  // Log current latency even when sla_admin is not used or no adjustment is needed
//  JSONWriter jwriter;
//  EventHelpers::AppendCurrentTime(&jwriter);
//  jwriter << "mutant_sla_admin_adjust";
//  jwriter.StartObject();
//  jwriter << "cur_lat" << lat;
//  jwriter << "slow_dev_r_iops" << slow_dev_r_iops;
//  jwriter << "slow_dev_w_iops" << slow_dev_w_iops;
//
//  boost::posix_time::ptime cur_time = boost::posix_time::microsec_clock::local_time();
//  _LogSstStatus(cur_time, &jwriter);
//
//  if (make_adjustment) {
//    // No adjustment when there is a write going on any of the SSTables
//    lock_guard<mutex> _(_last_sst_write_time_lock);
//    if (!_last_sst_write_time.is_not_a_date_time()) {
//      if ((cur_time - _last_sst_write_time).total_milliseconds() < _options.sst_ott_adj_cooldown_ms) {
//        jwriter << "adj_type" << "cool_down";
//        make_adjustment = false;
//      }
//    }
//  }
//
//  if (_options.sla_admin_type == "none" || !make_adjustment ) {
//    jwriter << "make_adjustment" << make_adjustment;
//    jwriter.EndObject();
//    jwriter.EndObject();
//    _logger->Log(jwriter);
//    return;
//  }
//
//  double cur_value = -1;
//  if (_options.sla_admin_type == "latency") {
//    cur_value = lat;
//  } else if (_options.sla_admin_type == "slow_dev_r_iops") {
//    cur_value = slow_dev_r_iops;
//  } else {
//    THROW("Unexpected");
//  }
//
//  _AdjSstOtt(cur_value, cur_time, &jwriter);
//
//  jwriter.EndObject();
//  jwriter.EndObject();
//  _logger->Log(jwriter);
//}


// Log SSTable status:
//   Current temperature
//   The current storage device
//   Where it should be based on the current sst_ott
// The number of SSTables in fast and slow devices.
// The number of SSTables that would be in fast and slow devices based on the current sst_ott.
//void Mutant::_LogSstStatus(const boost::posix_time::ptime& cur_time, JSONWriter* jwriter) {
//  int num_ssts_fast = 0;
//  int num_ssts_slow = 0;
//  int num_ssts_fast_should_be = 0;
//  int num_ssts_slow_should_be = 0;
//  vector<string> sst_status_str;
//  {
//    lock_guard<mutex> l_(_sstMapLock);
//    for (auto i: _sstMap) {
//      uint64_t sst_id = i.first;
//      SstTemp* st = i.second;
//      double temp = st->Temp(cur_time);
//      uint32_t path_id = st->PathId();
//
//      uint32_t path_id_should_be = 0;
//
//      if (path_id == 0) {
//        num_ssts_fast ++;
//      } else {
//        num_ssts_slow ++;
//      }
//      if (path_id_should_be == 0) {
//        num_ssts_fast_should_be ++;
//      } else {
//        num_ssts_slow_should_be ++;
//      }
//      sst_status_str.push_back(str(boost::format("%d:%d:%.3f:%d:%d")
//            % sst_id % st->Level() % temp % path_id % path_id_should_be));
//    }
//  }
//  (*jwriter) << "sst_status" << boost::algorithm::join(sst_status_str, " ");
//  (*jwriter) << "num_ssts_in_fast_dev" << num_ssts_fast;
//  (*jwriter) << "num_ssts_in_slow_dev" << num_ssts_slow;
//  (*jwriter) << "num_ssts_should_be_in_fast_dev" << num_ssts_fast_should_be;
//  (*jwriter) << "num_ssts_should_be_in_slow_dev" << num_ssts_slow_should_be;
//}


//void Mutant::_AdjSstOtt(double cur_value, const boost::posix_time::ptime& cur_time, JSONWriter* jwriter) {
//  // When there isn't enough data, make no adjustment.
//  bool make_adjustment = false;
//  if (_options.sla_admin_type == "latency") {
//    {
//      lock_guard<mutex> _(_lat_hist_lock);
//      if (_options.sla_observed_value_hist_q_size <= _lat_hist.size()) {
//        _lat_hist.pop_front();
//      }
//      _lat_hist.push_back(cur_value);
//      make_adjustment = (_options.sla_observed_value_hist_q_size <= _lat_hist.size());
//    }
//  } else if (_options.sla_admin_type == "slow_dev_r_iops") {
//    {
//      lock_guard<mutex> _(_slow_dev_r_iops_hist_lock);
//      if (_options.sla_observed_value_hist_q_size <= _slow_dev_r_iops_hist.size()) {
//        _slow_dev_r_iops_hist.pop_front();
//      }
//      _slow_dev_r_iops_hist.push_back(cur_value);
//      make_adjustment = (_options.sla_observed_value_hist_q_size <= _slow_dev_r_iops_hist.size());
//    }
//  }
//
//  (*jwriter) << "make_adjustment" << make_adjustment;
//  if (!make_adjustment)
//    return;
//
//  //double pid_adj = _sla_admin->CalcAdj(cur_value, *jwriter);
//  // When 0 < pid_adj, target_value is bigger. should increase slow dev iops. should raise sst_ott.
//
//  int sst_ott_adj = 0;
//  if (pid_adj < _options.error_adj_ranges[0]) {
//    // target_value < observed_value. should decrease slow dev iops. should lower sst_ott.
//    sst_ott_adj = -1;
//  } else if (pid_adj < _options.error_adj_ranges[1]) {
//    // pid_adj is within the error margin. make no adjustment.
//    (*jwriter) << "adj_type" << "no_change";
//    return;
//  } else {
//    // observed_value < target_value. should increase slow dev iops. should raise sst_ott.
//    sst_ott_adj = 1;
//  }
//
//  vector<double> sst_temps;
//  {
//    lock_guard<mutex> l_(_sstMapLock);
//    for (auto i: _sstMap) {
//      SstTemp* st = i.second;
//      sst_temps.push_back(st->Temp(cur_time));
//    }
//  }
//  sort(sst_temps.begin(), sst_temps.end());
//  int s = sst_temps.size();
//  if (s == 0) {
//    // No adjustment when there is no SSTable
//    (*jwriter) << "adj_type" << "no_sstable";
//    return;
//  }
//
//  // We make organizations by adjusting sst_ott little by little.
//  //
//  // Directly sorting SSTables by their temperatures and move one at the boundary may be intuitive, but won't work.
//  //   Because an SSTable temperature changes over time and some hot SSTables can be in slow dev or some cold SSTables can be in fast dev.
//
//  // Find where the current _sst_ott belongs in the sorted SSTable temperatures
//  int i = 0;
//  for ( ; i < s; i ++) {
//    if (_sst_ott < sst_temps[i])
//      break;
//  }
//  // Now (sst_temps[i - 1] <= _sst_ott) and (_sst_ott < sst_temps[i])
//  if (sst_ott_adj == 1) {
//    i ++;
//  } else if (sst_ott_adj == -1) {
//    i --;
//  }
//
//  double new_sst_ott;
//  if (i <= 0) {
//    new_sst_ott = 0.0;
//    (*jwriter) << "adj_type" << "no_change_lowest";
//  } else if (s <= i) {
//    new_sst_ott = sst_temps[s-1] + 1.0;
//    (*jwriter) << "adj_type" << "no_change_highest";
//  } else {
//    if (sst_ott_adj == 1) {
//      (*jwriter) << "adj_type" << "move_sst_to_slow";
//    } else if (sst_ott_adj == -1) {
//      (*jwriter) << "adj_type" << "move_sst_to_fast";
//    }
//    // Take the average when (sst_temps[i - 1] <= _sst_ott) and (_sst_ott < sst_temps[i])
//    new_sst_ott = (sst_temps[i-1] + sst_temps[i]) / 2.0;
//  }
//  _sst_ott = new_sst_ott;
//}


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

  //if (_sla_admin) {
  //  delete _sla_admin;
  //  _sla_admin = nullptr;
  //}

  //if (_disk_mon) {
  //  delete _disk_mon;
  //  _disk_mon = nullptr;
  //}

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


//void Mutant::SlaAdminInit(double target_lat, double p, double i, double d) {
//  static Mutant& i_ = _GetInst();
//  i_._SlaAdminInit(target_lat, p, i, d);
//}


//void Mutant::SlaAdminAdjust(double lat) {
//  static Mutant& i = _GetInst();
//  i._SlaAdminAdjust(lat);
//}


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
