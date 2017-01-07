#pragma once

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <string.h>

#include <fstream>
#include <mutex>
#include <string>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/format.hpp>

namespace Util {
	const std::string& HomeDir();

	std::string ToYMD_HMS(const boost::posix_time::time_duration& td);

	std::string Indent(const std::string& in, int indent);

	std::string Prepend(const std::string& p, const std::string& in);

	void RunSubprocess(const std::string& cmd_);

	void SetEnv(const char* k, const char* v);
	void SetEnv(const char* k, const std::string& v);

	void ReadStr(std::ifstream& ifs, std::string& str);

	const std::string& SrcDir();

	double ArcInRadians(double lat0, double lon0, double lat1, double lon1);
	double ArcInMeters(double lat0, double lon0, double lat1, double lon1);

	void Ll_3Dc(const double lat, const double lon, double xyz[]);

	std::string exec(const std::string& cmd);

	std::string StackTrace(int skip_innermost_stack);

	std::string CurDateTime();

	std::string BuildHeader(const std::string& fmt, const std::string& column_names);
};

class _Error : public std::runtime_error {
	const char* file_name;
	const int line_no;
	std::string _what;
	void _Init();

public:
  // Prevent threshing when multiple threads hit the same exception code
  static std::mutex _mutex;

	_Error(const std::string& s, const char* file_name_, const int line_no_);
	_Error(boost::format& f, const char* file_name_, const int line_no_);
	const char* what() const noexcept;
};

#define THROW(m) throw _Error((m), __FILE__, __LINE__)

#define TRACE std::cout << "TRACE: " << basename((char*) (__FILE__)) << " " << __LINE__ << " "
