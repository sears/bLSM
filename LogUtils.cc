/*! \file log4_util.cc
 *  \brief This file has the helper functions for log4cpp;
 *
 *  Copyright (c) 2008 Yahoo, Inc.
 *  All rights reserved.
 */
#include <iostream>
#include <log4cpp/PropertyConfigurator.hh>

#include "LogUtils.h"

using namespace log4cpp;
using namespace std;

// hacked link to actioncontext
std::string s_trackPathLog;

LogMethod::
LogMethod(log4cpp::Category& log, log4cpp::Priority::Value priority,
          const char *function) :
    log_(log), priority_(priority), function_(function)
{
    if(log_.isPriorityEnabled(priority_)) {
        log_.getStream(priority_) << "Entering: " << function_;
    }
}


LogMethod::
~LogMethod()
{
    if(log_.isPriorityEnabled(priority_)) {
        log_.getStream(priority_) << "Exiting: " << function_;
    }
}

// Protects against multiple calls (won't try to re-init) and gives
// back the same answer the original call got.
static int log4cppInitResult = -1;

bool
initLog4cpp(const string &confFile)
{

    if (log4cppInitResult != -1) {
        return (log4cppInitResult == 0 ? true : false);
    }

    log4cppInitResult = 0; // Assume success.
    try {
        PropertyConfigurator::configure(confFile);
    } catch (log4cpp::ConfigureFailure &e) {
        cerr << "log4cpp configuration failure while loading '" <<
            confFile << "' : " << e.what() << endl;
        log4cppInitResult = 1;
    } catch (std::exception &e) {
        cerr << "exception caught while configuring log4cpp via '" <<
            confFile << "': " << e.what() << endl;
        log4cppInitResult = 1;
    } catch (...) {
        cerr << "unknown exception while configuring log4cpp via '" <<
            confFile << "'." << endl;
        log4cppInitResult = 1;
    }

    return (log4cppInitResult == 0 ? true : false);
}

/*
 * For customized vim control
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4:ts=4:et
 * vim<600: sw=4:ts=4:et
 */
