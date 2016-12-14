#pragma once

#include <string>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <yaml-cpp/yaml.h>

namespace Conf {
	void Init(int argc, char* argv[]);

	YAML::Node Get(const std::string& k);
	std::string GetStr(const std::string& k);
};
