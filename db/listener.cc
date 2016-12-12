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

}
