#include <execinfo.h>
#include <libgen.h>
#include <math.h>
#include <pwd.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/regex.hpp>

//#include "cons.h"
#include "util.h"

using namespace std;


namespace Util {
	const string& HomeDir() {
		static string homedir;
		if (homedir.size() == 0) {
			struct passwd *pw = getpwuid(getuid());
			homedir = pw->pw_dir;
		}
		return homedir;
	}

	string ToYMD_HMS(const boost::posix_time::time_duration& td) {
		// in: 7027:33:44
		// out: 1y0m3d-19:33:44
		static const auto sep = boost::is_any_of(":");
		vector<string> t;
		string s = boost::posix_time::to_simple_string(td);
		boost::split(t, s, sep);

		int h = atoi(t[0].c_str());	// total hours
		int year = h / (365 * 24);	// starts with 0
		h -= (year * (365 * 24));	// remaining hours
		const double hours_in_month = 365.0/12*24;
		int month = int(h / hours_in_month);	// starts with 0
		h -= int(month * hours_in_month);
		int day = h / 24;
		h -= (day * 24);
		int hour = h;

		string out;

		bool started = false;
		if (year == 0 && started == false)
			;
		else {
			out += str(boost::format("%dy") % year);
			started = true;
		}

		if (month == 0 && started == false)
			;
		else {
			out += str(boost::format("%dm") % month);
			started = true;
		}

		if (day == 0 && started == false)
			;
		else {
			out += str(boost::format("%dd-") % day);
			started = true;
		}

		out += str(boost::format("%02d:%s:%02d") % hour % t[1] % atoi(t[2].c_str()));
		return out;
	}


	string Indent(const string& in, int indent) {
		//cout << "[" << in << "]\n";
		static const auto sep = boost::is_any_of("\n");

		string leading_spaces;
		for (int i = 0; i < indent; i ++)
			leading_spaces += " ";

		vector<string> tokens;
		boost::split(tokens, in, sep);
		string out;

		for (size_t i = 0; i < tokens.size(); i ++) {
			if (i != 0)
				out += "\n";
			if (tokens[i].size() != 0)
				out += (leading_spaces + tokens[i]);
		}

		return out;
	}

	string Prepend(const string& p, const string& in) {
		static const auto sep = boost::is_any_of("\n");
		vector<string> tokens;
		boost::split(tokens, in, sep);
		string out;

		for (size_t i = 0; i < tokens.size(); i ++) {
			if ((i == tokens.size() - 1) && tokens[i].size() == 0)
				continue;
			out += (p + tokens[i] + "\n");
		}
		return out;
	}

	//void RunSubprocess(const string& cmd_) {
	//	Cons::MT _(cmd_, false);
	//	string cmd = string("( ") + cmd_ + " ) 2>&1";
	//	FILE* pipe = popen(cmd.c_str(), "r");
	//	if (! pipe)
	//		THROW(boost::format("Unable to popen: %1%") % cmd);

	//	char buffer[4096];
	//	while (! feof(pipe)) {
	//		// Print as soon as a line is available
	//		if (fgets(buffer, sizeof(buffer), pipe))
	//			Cons::P(buffer);
	//	}
	//	pclose(pipe);
	//}

	void SetEnv(const char* k, const char* v) {
		if (setenv(k, v, 1) != 0)
			THROW(boost::format("Unable to setenv: %s %s") % k % v);
	}

	void SetEnv(const char* k, const string& v) {
		if (setenv(k, v.c_str(), 1) != 0)
			THROW(boost::format("Unable to setenv: %s %s") % k % v);
	}

	void ReadStr(std::ifstream& ifs, std::string& str) {
		size_t s;
		ifs.read((char*)&s, sizeof(s));
		str.resize(s);
		ifs.read((char*)&str[0], s);
	}

	const string& SrcDir() {
		static string srcdir;
		if (srcdir.size() == 0) {
			char file[PATH_MAX];
			strcpy(file, __FILE__);
			srcdir = dirname(file);
		}
		return srcdir;
	}

	// http://en.wikipedia.org/wiki/Haversine_formula
	// http://blog.julien.cayzac.name/2008/10/arc-and-distance-between-two-points-on.html

	/** @brief Computes the arc, in radian, between two WGS-84 positions.
	 *
	 * The result is equal to <code>Distance(from,to)/EARTH_RADIUS_IN_METERS</code>
	 *    <code>= 2*asin(sqrt(h(d/EARTH_RADIUS_IN_METERS )))</code>
	 *
	 * where:<ul>
	 *    <li>d is the distance in meters between 'from' and 'to' positions.</li>
	 *    <li>h is the haversine function: <code>h(x)=sin²(x/2)</code></li>
	 * </ul>
	 *
	 * The haversine formula gives:
	 *    <code>h(d/R) = h(from.lat-to.lat)+h(from.lon-to.lon)+cos(from.lat)*cos(to.lat)</code>
	 *
	 * @sa http://en.wikipedia.org/wiki/Law_of_haversines
	 */
	double ArcInRadians(
			double lat0, double lon0,
			double lat1, double lon1) {
		/// @brief The usual PI/180 constant
		static const double DEG_TO_RAD = 0.017453292519943295769236907684886;
		/// @brief Earth's quatratic mean radius for WGS-84
		//static const double EARTH_RADIUS_IN_METERS = 6372797.560856;

		double latitudeArc  = (lat0 - lat1) * DEG_TO_RAD;
		double longitudeArc = (lon0 - lon1) * DEG_TO_RAD;
		double latitudeH = sin(latitudeArc * 0.5);
		latitudeH *= latitudeH;
		double lontitudeH = sin(longitudeArc * 0.5);
		lontitudeH *= lontitudeH;
		double tmp = cos(lat0 * DEG_TO_RAD) * cos(lat1 * DEG_TO_RAD);
		return 2.0 * asin(sqrt(latitudeH + tmp*lontitudeH));
	}

	double ArcInMeters(
			double lat0, double lon0,
			double lat1, double lon1) {
		static const double EARTH_RADIUS_IN_METERS = 6372797.560856;
		return EARTH_RADIUS_IN_METERS * ArcInRadians(lat0, lon0, lat1, lon1);
	}

	// Latitude/longitude to 3D Cartesian coordinates
	void Ll_3Dc(const double lat, const double lon, double xyz[]) {
		// http://stackoverflow.com/questions/1185408/converting-from-longitude-latitude-to-cartesian-coordinates

		double lat_r = M_PI * (lat / 180);
		double lon_r = M_PI * (lon / 180);

		static const double R = 1.0;
		xyz[0] = R * cos(lat_r) * cos(lon_r);
		xyz[1] = R * cos(lat_r) * sin(lon_r);
		xyz[2] = R * sin(lat_r);
	}

	// http://stackoverflow.com/questions/478898/how-to-execute-a-command-and-get-output-of-command-within-c-using-posix
	string exec(const string& cmd) {
		const int buf_size = 1024;
		char buffer[buf_size];
		string result = "";
		shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
		if (!pipe) THROW("popen() failed!");
		while (!feof(pipe.get())) {
			if (fgets(buffer, buf_size, pipe.get()) != NULL)
				result += buffer;
		}
		return result;
	}

	string StackTrace(int skip_innermost_stack) {
		const int max_bufs = 32;
		void* buf[max_bufs];
		int buf_size = backtrace(buf, max_bufs);

		char** messages = backtrace_symbols(buf, buf_size);
		string stack_trace;
		for (int i = skip_innermost_stack; i < buf_size; ++ i) {
			if (i > skip_innermost_stack)
				stack_trace += "\n";
			//Cons::P(messages[i]);

			// Examples:
			//   target/simulator(_ZN10LocationDB7WhereIsERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE+0x65) [0x4b0261]
			//   0                1                                                                                   234        56
			//   There must be a new line or something at the end.
			//
			//   /lib/x86_64-linux-gnu/libc.so.6(+0x354a0) [0x7f17a1f6b4a0]
			//   0                               1        234              56
			//
			//   target/clusterer() [0x46b199]
			//   0                1234        56
			//   target/clusterer(_ZN9Clusterer5PointC2Ev+0xb1) [0x46d7cf]
			//   0                1                            234        56
			static const auto sep = boost::is_any_of("() \t[]\r\n");
			vector<string> t;
			string m1(boost::trim_copy(string(messages[i])));
			boost::split(t, m1, sep, boost::token_compress_off);
			if (t.size() != 6) {
				THROW(boost::format("unexpected format: %s\nt.size()=%d\nt=[%s]")
							% messages[i] % t.size() % boost::join(t, "\n"));
			}

			const string& binary_file_name = t[0];

			string decorated_function_name;
			if (t[1].size() > 0) {
				static const auto sep1 = boost::is_any_of("+");
				vector<string> t1;
				boost::split(t1, t[1], sep1);
				if (t1.size() != 2) {
					THROW(boost::format("unexpected format: %s\nt1.size()=%d\nt1=[%s]")
								% messages[i] % t1.size() % boost::join(t1, "\n"));
				}
				decorated_function_name = t1[0];
			}

			stack_trace += messages[i];
			stack_trace += "\n";
			//Cons::P(boost::format("DF: %s") % decorated_function_name);

			if (decorated_function_name.size() > 0) {
				// c++filt _ZN10LocationDB7WhereIsERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE
				stack_trace += Util::Indent(boost::trim_copy(Util::exec(str(boost::format("c++filt %s") % decorated_function_name))), 2);
				stack_trace += "\n";
			}

			string addr = t[4];
			//Cons::P(boost::format("addr: %s") % addr);
			if (addr.size() > 0) {
				stack_trace += Util::Indent(boost::trim_copy(Util::exec(str(boost::format("addr2line -e %s %s") % binary_file_name % addr))), 2);
			}

			stack_trace += "\n";
		}

		return stack_trace;
	}

	string CurDateTime() {
		time_t     now = time(0);
		struct tm  tm_;
		char       buf[80];
		tm_ = *localtime(&now);
		//strftime(buf, sizeof(buf), "%m%d-%H%M%S", &tm_);
		strftime(buf, sizeof(buf), "%m%d-%H%M", &tm_);

		// Add seconds and milliseconds.
		struct timeval tp;
		gettimeofday(&tp, NULL);
		//long int ms = tp.tv_sec * 1000 + tp.tv_usec / 1000;
		return str(boost::format("%s%02d.%03d")
			% buf
			% (tp.tv_sec % 60)
			% (tp.tv_usec / 1000));
	}

	string BuildHeader(const string& fmt, const string& column_names) {
		// Float, integer, or string
		static const boost::regex e("%(([-+]?[0-9]*\\.?[0-9]*f)|([-+]?[0-9]*d)|([-+]?[0-9]*s))");

		boost::smatch m;
		string fmt0 = fmt;
		vector<size_t> name_end_pos;
		size_t nep = 0;
		while (boost::regex_search(fmt0, m, e)) {
			if (m.size() == 0)
				THROW("Unexpected");
			if (nep != 0)
				nep ++;
			nep += abs(atoi(m[0].str().c_str() + 1));
			name_end_pos.push_back(nep);
			fmt0 = m.suffix();
		}

		//cout << "name_end_pos:\n";
		//for (int i: name_end_pos)
		//	cout << i << " ";
		//cout << "\n";

		vector<string> names;
		static const auto sep = boost::is_any_of(" ");
		boost::split(names, column_names, sep, boost::token_compress_on);
		//string names_flat("#");
		//for (auto n: names)
		//	names_flat += (" " + n);
		//names_flat += "\n";
		//names_flat += "#\n";

		// Header lines
		vector<string> lines;
		for (size_t i = 0; i < names.size(); i ++) {
			bool fit = false;
			for (auto& l: lines) {
				if (l.size() + 1 + names[i].size() > name_end_pos[i])
					continue;

				while (l.size() + 1 + names[i].size() < name_end_pos[i])
					l += " ";
				l += (" " + names[i]);
				fit = true;
				break;
			}

			if (fit)
				continue;

			string l("#");
			while (l.size() + 1 + names[i].size() < name_end_pos[i])
				l += " ";
			l += (" " + names[i]);
			lines.push_back(l);
		}

		// Indices for names starting from 1 for easy gnuplot indexing
		vector<string> ilines;
		for (size_t i = 0; i < names.size(); i ++) {
			string idxstr = str(boost::format("%d") % (i + 1));
			bool fit = false;
			for (auto& l: ilines) {
				if (l.size() + 1 + idxstr.size() > name_end_pos[i])
					continue;

				while (l.size() + 1 + idxstr.size() < name_end_pos[i])
					l += " ";
				l += (" " + idxstr);
				fit = true;
				break;
			}

			if (fit)
				continue;

			string l("#");
			while (l.size() + 1 + idxstr.size() < name_end_pos[i])
				l += " ";
			l += (" " + idxstr);
			ilines.push_back(l);
		}

		//string header = names_flat;
		string header;
		for (auto l: lines)
			header += (l + "\n");
		for (auto l: ilines)
			header += l;

		return header;
	}
};


_Error::_Error(const std::string& s, const char* file_name_, const int line_no_)
: runtime_error(s), file_name(file_name_), line_no(line_no_)
{
	_Init();
}

_Error::_Error(boost::format& f, const char* file_name_, const int line_no_)
: runtime_error(str(f)), file_name(file_name_), line_no(line_no_)
{
	_Init();
}

void _Error::_Init() {
  lock_guard<mutex> _(_mutex);
	// Stack trace skips the innermost 3 functions, StackTrace(), _Init(), and
	// _Error().
	_what = str(boost::format("%s\n%s")
			% runtime_error::what()
			% Util::Indent(Util::StackTrace(3), 2));
}

const char* _Error::what() const noexcept {
	return _what.c_str();
}

std::mutex _Error::_mutex;
