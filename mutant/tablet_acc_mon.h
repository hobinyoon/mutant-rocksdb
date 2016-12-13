#pragma once

#include <atomic>
#include <map>
#include <mutex>

namespace rocksdb {

class _MemtAccCnt;
class _SstAccCnt;

class TabletAccMon {
  std::atomic<bool> _updatedSinceLastOutput;

  // SSTable access monitoring is for the SSTable migration decisions.
  // MemTable access monitoring is just to see/understand what's going on
  // internally.
  std::map<void*, _MemtAccCnt*> _memtAccCnt;
  std::mutex _memtAccCntLock;
  std::map<uint64_t, _SstAccCnt*> _sstAccCnt;
  std::mutex _sstAccCntLock;

  static TabletAccMon& _GetInst();

  TabletAccMon();

  void _MemtRead(void* m);
  void _SstRead(uint64_t s);

public:
  static void MemtRead(void* m);
  static void SstRead(uint64_t s);
};

}
