// Console output utility

#include <string>
#include <iostream>
#include <boost/algorithm/string.hpp>

#include "cons.h"

using namespace std;

namespace Cons {
	int _ind_len = 0;
	string _ind = "";

	// Note: when thread-safety is needed
	//_print_lock = threading.Lock()

	void P(const string& s, int ind) {
		//with _print_lock:
		
		if (ind > 0) {
			_ind_len += ind;
			for (int i = 0; i < ind; i ++)
				_ind += " ";
		}

		static const auto sep = boost::is_any_of("\n");

		if (_ind_len > 0) {
			vector<string> lines;
			boost::split(lines, s, sep);

			for (size_t i = 0; i < lines.size(); i ++) {
				// Don't indent the last line if it is blank
				if (((i+1) == lines.size()) && (lines[i].length() == 0))
					continue;
				cout << _ind << lines[i] << "\n";
			}
		} else {
			cout << s << "\n";
		}

		if (ind > 0) {
			_ind_len -= ind;
			_ind = _ind.substr(0, _ind_len);
		}
	}

	void P(const boost::format& f, int ind) {
		P(str(f), ind);
	}

	void P(const char* s, int ind) {
		P(string(s), ind);
	}

	void P(const double d, int ind) {
		P(to_string(d), ind);
	}

	void P(const int i, int ind) {
		P(to_string(i), ind);
	}

	void P(const size_t i, int ind) {
		P(to_string(i), ind);
	}

	void P(const bool b, int ind) {
		string s = (b ? "true" : "false");
		P(s, ind);
	}

	void Pnnl(const string& s, int ind) {
		if (ind > 0) {
			_ind_len += ind;
			for (int i = 0; i < ind; i ++)
				_ind += " ";
		}

		static const auto sep = boost::is_any_of("\n");

		if (_ind_len > 0) {
			vector<string> lines;
			boost::split(lines, s, sep);

			for (size_t i = 0; i < lines.size(); i ++) {
				// Don't indent the last line if it is blank
				if (((i+1) == lines.size()) && (lines[i].length() == 0))
					continue;
				cout << _ind << lines[i] << flush;
			}
		} else {
			cout << s << flush;
		}

		if (ind > 0) {
			_ind_len -= ind;
			_ind = _ind.substr(0, _ind_len);
		}
	}

	void Pnnl(const char* s, int ind) {
		Pnnl(string(s), ind);
	}

	void Pnnl(const boost::format& f, int ind) {
		Pnnl(str(f), ind);
	}

	void Pnnl(const double d, int ind) {
		Pnnl(to_string(d), ind);
	}

	void Pnnl(const int i, int ind) {
		Pnnl(to_string(i), ind);
	}

	void Pnnl(const bool b, int ind) {
		string s = (b ? "true" : "false");
		Pnnl(s, ind);
	}

	void ClearLine() {
		// http://en.wikipedia.org/wiki/ANSI_escape_code
		//static const String ESC = "\033[";
		#define ESC "\033["
		cout << ESC "1K"	// clear from cursor to beginning of the line
			ESC "1G"; // move the cursor to column 1
	}

	void EraseChar() {
		// http://stackoverflow.com/questions/12765297/erasing-using-backspace-control-character
		cout << "\b \b";
	}

	void MT::_Init(const string& msg_, bool print_time_) {
		msg = msg_;
		P(msg);
		print_time = print_time_;

		_ind_len += 2;
		_ind += "  ";
		if (print_time) {
			tmr = new boost::timer::cpu_timer();
		} else {
			tmr = NULL;
		}
	}

	MT::MT(const string& msg, bool print_time) {
		_Init(msg, print_time);
	}

	MT::MT(const boost::format& f, bool print_time) {
		_Init(str(f), print_time);
	}

	MT::~MT() {
		if (print_time) {
			P(boost::format("%.0f ms") % (tmr->elapsed().wall / 1000000.0));
			delete tmr;
		}
		_ind_len -= 2;
		_ind = _ind.substr(0, _ind_len);
	}

	void MTnnl::_Init(const string& msg_, bool print_time_) {
		msg = msg_;
		Pnnl(msg);
		print_time = print_time_;

		_ind_len += 2;
		_ind += "  ";
		if (print_time) {
			tmr = new boost::timer::cpu_timer();
		} else {
			tmr = NULL;
		}
	}

	MTnnl::MTnnl(const string& msg, bool print_time) {
		_Init(msg, print_time);
	}

	MTnnl::MTnnl(const boost::format& f, bool print_time) {
		_Init(str(f), print_time);
	}

	MTnnl::~MTnnl() {
		if (print_time) {
			P(boost::format("%.0f ms") % (tmr->elapsed().wall / 1000000.0));
			delete tmr;
		}
		_ind_len -= 2;
		_ind = _ind.substr(0, _ind_len);
	}


//# No new-line
//class MTnnl:
//	def __init__(self, msg):
//		self.msg = msg
//
//	def __enter__(self):
//		global _ind_len, _ind
//		Pnnl(self.msg)
//		_ind_len += 2
//		_ind += "  "
//		self.start_time = time.time()
//		return self
//
//	def __exit__(self, type, value, traceback):
//		global _ind_len, _ind
//		dur = time.time() - self.start_time
//		P("%.0f ms" % (dur * 1000.0))
//		_ind_len -= 2
//		_ind = _ind[: len(_ind) - 2]


	void Test() {
		P("aa");

		{
			MT _("dkdkdk");
			P(1.5);
			P(true);
		}

		P("aa\nbb\n\n cc\n\n  dd");
		P(1);
	}
};


//def sys_stdout_write(msg):
//	with _print_lock:
//		sys.stdout.write(msg)
//		sys.stdout.flush()


//class Indent:
//	def __init__(self, msg):
//		self.msg = msg
//
//	def __enter__(self):
//		global _ind_len, _ind
//		P(self.msg)
//		_ind_len += 2
//		_ind += "  "
//		return self
//
//	def __exit__(self, type, value, traceback):
//		global _ind_len, _ind
//		_ind_len -= 2
//		_ind = _ind[: len(_ind) - 2]
