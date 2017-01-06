#include <iostream>
#include "rocksdb/listener.h"

namespace rocksdb {

std::ostream& operator << (std::ostream& s, const TableFileCreationReason r) {
	if (r == TableFileCreationReason::kFlush) {
		s << "kFlush";
	} else if (r == TableFileCreationReason::kCompaction) {
		s << "kCompaction";
	} else if (r == TableFileCreationReason::kRecovery) {
		s << "kRecovery";
	} else {
		s << "UNDEFINED";
	}
	return s;
}

std::ostream& operator<< (std::ostream& os, const CompactionReason& c) {
  if (c == CompactionReason::kUnknown) os << "kUnknown";
  else if (c == CompactionReason::kLevelL0FilesNum) os << "kLevelL0FilesNum";
  else if (c == CompactionReason::kLevelMaxLevelSize) os << "kLevelMaxLevelSize";
  else if (c == CompactionReason::kUniversalSizeAmplification) os << "kUniversalSizeAmplification";
  else if (c == CompactionReason::kUniversalSizeRatio) os << "kUniversalSizeRatio";
  else if (c == CompactionReason::kUniversalSortedRunNum) os << "kUniversalSortedRunNum";
  else if (c == CompactionReason::kFIFOMaxSize) os << "kFIFOMaxSize";
  else if (c == CompactionReason::kManualCompaction) os << "kManualCompaction";
  else if (c == CompactionReason::kFilesMarkedForCompaction) os << "kFilesMarkedForCompaction";
  else os << "UNEXPECTED";
  return os;
}

}
