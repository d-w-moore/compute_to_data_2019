// =-=-=-=-=-=-=-
// irods includes
#include "irods_error.hpp"
#include "irods_ms_plugin.hpp"

// =-=-=-=-=-=-=-
// stl includes
#include <cstring>
#include <cstdlib>
#include <string>
#include <cstring>
#include <regex.h>

int msiregister_as_admin(
    msParam_t*      _param1,
    msParam_t*      _param2,
    ruleExecInfo_t* _rei ) {

    const char* str = parseMspForStr(_param1);
    std::string s {str};
    s += ".moretext";
    fillStrInMsParam(_param1, s.c_str());
    return 0;
}

extern "C"
irods::ms_table_entry* plugin_factory() {
    irods::ms_table_entry* msvc = new irods::ms_table_entry(2);
    msvc->add_operation<
        msParam_t*,
        msParam_t*,
        ruleExecInfo_t*>("msiregister_as_admin",
                         std::function<int(
                             msParam_t*,
                             msParam_t*,
                             ruleExecInfo_t*)>(msiregister_as_admin));
    return msvc;
}

