#pragma once

#include <string>
#include <boost/format.hpp>
#include <boost/timer/timer.hpp>

namespace Cons {
	void P(const std::string& s, int ind = 0);
	void P(const char* s, int ind = 0);
	void P(const boost::format& f, int ind = 0);
	void P(const double d, int ind = 0);
	void P(const int i, int ind = 0);
	void P(const size_t i, int ind = 0);
	void P(const bool b, int ind = 0);

	void Pnnl(const std::string& s, int ind = 0);
	void Pnnl(const char* s, int ind = 0);
	void Pnnl(const boost::format& f, int ind = 0);
	void Pnnl(const double d, int ind = 0);
	void Pnnl(const int i, int ind = 0);
	void Pnnl(const bool b, int ind = 0);

	void ClearLine();
	void EraseChar();

	// Measure time
	struct MT {
		private:
			std::string msg;
			bool print_time;
			boost::timer::cpu_timer* tmr;
			void _Init(const std::string& msg, bool print_time);

		public:
			MT(const std::string& msg, bool print_time = true);
			MT(const boost::format& f, bool print_time = true);
			~MT();
	};

	// No newline version. Hard to inherit from MT since the instances are not
	// dynamically allocated. Might be possible with some pre-processor hack.
	struct MTnnl {
		private:
			std::string msg;
			bool print_time;
			boost::timer::cpu_timer* tmr;
			void _Init(const std::string& msg, bool print_time);

		public:
			MTnnl(const std::string& msg, bool print_time = true);
			MTnnl(const boost::format& f, bool print_time = true);
			~MTnnl();
	};

	void Test();
};
