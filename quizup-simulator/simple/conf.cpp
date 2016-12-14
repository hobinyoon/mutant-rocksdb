#include <iostream>
#include <string>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/filesystem.hpp>
#include <boost/format.hpp>
#include <boost/program_options.hpp>
#include <boost/regex.hpp>

#include "conf.h"
#include "cons.h"
#include "util.h"


using namespace std;

namespace Conf {
	YAML::Node _yaml_root;

	void _LoadYaml() {
		string fn = str(boost::format("%s/config.yaml") % boost::filesystem::path(__FILE__).parent_path().string());
		_yaml_root = YAML::LoadFile("config.yaml");
	}

	namespace po = boost::program_options;

	template<class T>
	void __EditYaml(const string& key, po::variables_map& vm) {
		if (vm.count(key) != 1)
			return;
		T v = vm[key].as<T>();
		static const auto sep = boost::is_any_of(".");
		vector<string> tokens;
		boost::split(tokens, key, sep, boost::token_compress_on);
		// Had to use a pointer to traverse the tree. Otherwise, the tree gets
		// messed up.
		YAML::Node* n = &_yaml_root;
		for (string t: tokens) {
			YAML::Node n1 = (*n)[t];
			n = &n1;
		}
		*n = v;
		//Cons::P(Desc());
	}

	void _ParseArgs(int argc, char* argv[]) {
		po::options_description od("Allowed options");
		od.add_options()
			("help", "show help message")
			;

		po::variables_map vm;
		po::store(po::command_line_parser(argc, argv).options(od).run(), vm);
		po::notify(vm);

		if (vm.count("help") > 0) {
			// well... this doesn't show boolean as string.
			cout << std::boolalpha;
			cout << od << "\n";
			exit(0);
		}
	}

	void Init(int argc, char* argv[]) {
		_LoadYaml();
		_ParseArgs(argc, argv);
	}

	YAML::Node Get(const std::string& k) {
		return _yaml_root[k];
	}

	string GetStr(const std::string& k) {
		return _yaml_root[k].as<string>();
	}
};
