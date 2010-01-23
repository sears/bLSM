/* Copyright (C) 2007 Yahoo! Inc. All Rights Reserved. */

#ifndef LOG_UTIL_H
#define LOG_UTIL_H

#include <log4cpp/Category.hh>
#include "StringUtils.h"

/**
 * Quick and dirty link between LogUtils and ActionContext without having to 
 * resolve cross-inclusion issues, or force all components to start including
 * ActionContext if they don't already.
 */
extern std::string s_trackPathLog;

// These macros cannot be protected by braces because of the trailing stream
// arguments that get appended.  Care must taken not to use them inside if/else 
// blocks that do not use curly braces.
// I.e., the following will give unexpected results:
// if(foo)
//   DHT_DEBUG_STREAM() << "heyheyhey";
// else
//   blah();
// The 'else' will end up applying to the 'if' within the debug macro.
// Regardless of this, our standards say to always use curly brackets
// on every block anyway, no matter what.

#define DHT_DEBUG_STREAM() if(log.isDebugEnabled()) log.debugStream() << __FUNCTION__ << "():" <<  __LINE__ << ":"
#define DHT_INFO_STREAM() if(log.isInfoEnabled()) log.infoStream() <<  __FUNCTION__ << "():" << __LINE__ << ":"
#define DHT_INFO_WITH_STACK_STREAM() if(log.isInfoEnabled()) log.infoStream() <<  __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_WARN_STREAM() if(log.isWarnEnabled()) log.warnStream() << __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_ERROR_STREAM() if(log.isErrorEnabled()) log.errorStream() << __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_CRIT_STREAM() if(log.isCritEnabled()) log.critStream() << __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_TRACE_PRIORITY log4cpp::Priority::DEBUG + 50
#define DHT_TRACE_STREAM() if (log.isPriorityEnabled(DHT_TRACE_PRIORITY)) log.getStream(DHT_TRACE_PRIORITY) <<  __FUNCTION__ << "():" << __LINE__ << ":"

// Sadly, sometimes 'log' is reserved by someone else so the code needs to
// use a different name for log.  In that case, it can be passed in to these.
#define DHT_DEBUG_STREAML(x_log_hdl_x) if((x_log_hdl_x).isDebugEnabled()) (x_log_hdl_x).debugStream() <<  __FUNCTION__ << "():" << __LINE__ << ":"
#define DHT_INFO_STREAML(x_log_hdl_x) if((x_log_hdl_x).isInfoEnabled()) (x_log_hdl_x).infoStream() <<  __FUNCTION__ << "():" << __LINE__ << ":"
#define DHT_INFO_WITH_STACK_STREAML(x_log_hdl_x) if((x_log_hdl_x).isInfoEnabled()) (x_log_hdl_x).infoStream() <<  __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_WARN_STREAML(x_log_hdl_x) if((x_log_hdl_x).isWarnEnabled()) (x_log_hdl_x).warnStream() << __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_ERROR_STREAML(x_log_hdl_x) if((x_log_hdl_x).isErrorEnabled()) (x_log_hdl_x).errorStream() << __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_CRIT_STREAML(x_log_hdl_x) if((x_log_hdl_x).isCritEnabled()) (x_log_hdl_x).critStream() << __FUNCTION__ << "():" << __LINE__ << ":" << s_trackPathLog
#define DHT_TRACE_STREAML(x_log_hdl_x) if ((x_log_hdl_x).isPriorityEnabled(DHT_TRACE_PRIORITY)) (x_log_hdl_x).getStream(DHT_TRACE_PRIORITY) <<  __FUNCTION__ << "():" << __LINE__ << ":"

//Macros to use when a function returns on error without writing any log message
// or error translation
#define RETURN_IF_NOT_OK(x_call_x) \
{ \
  FwCode::ResponseCode rcx___ = (x_call_x); \
  if(rcx___ != FwCode::FwOk) {   \
    return rcx___; \
  } \
}

#define RETURN_THIS_IF_NOT_OK(x_othercode_x, x_call_x)   \
{ \
  FwCode::ResponseCode rcx___ = (x_call_x); \
  if(rcx___ != FwCode::FwOk) {   \
      return (x_othercode_x);    \
  } \
}

/// Caution!  Only use in checks for 'impossible' code conditions.  Regular errors
/// should be handled regularly
#define BAD_CODE_ABORT() \
    { \
        std::string x_msg_x("Bad code at " __FILE__ ":"); \
        x_msg_x.append(StringUtils::toString(__LINE__)); \
        throw std::runtime_error(x_msg_x); \
    }

#define BAD_CODE_IF_NOT_OK(x_call_x) \
    do {\
    if((x_call_x) != FwCode::FwOk) { \
        BAD_CODE_ABORT(); \
    } \
    } while(0)

/*
 * Above macros are meant to be used by all components.
 */

/**
 * Class that allows for method entry/exit logging with a single declaration.
 * Always uses debug.
 */
class LogMethod
{
 public:
    LogMethod(log4cpp::Category& log, log4cpp::Priority::Value priority,
              const char *function);
    virtual ~LogMethod();

 private:
    log4cpp::Category& log_;
    log4cpp::Priority::Value priority_;
    const char *function_;
};

// convenience macros to use the above class
#define LOG_METHOD() LogMethod log_method_entry_exit(log, log4cpp::Priority::DEBUG, __FUNCTION__)
#define TRACE_METHOD() LogMethod log_method_entry_exit(log, DHT_TRACE_PRIORITY, __FUNCTION__)

/** Initialize log4cpp config file.
 * This function needs to be called once for each executable. Multiple
 * initializations will return the result of the first initialization (IOW,
 * an executable can be initialized with exactly one config file). Errors
 * encountered by this function are printed onto cerr. See log4cpp
 * documentation for what happens when PropertyConfigurator::configure()
 * fails.
 * \param confFile is the path name of the log4cpp config file.
 * Depending on the machine that the executable is running in, the path
 * will be different.
 * \return true if the initialization succeeds, false if it fails.
 */
bool initLog4cpp(const std::string & confFile);

#endif

/*
 * For customized vim control
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4:ts=4:et
 * vim<600: sw=4:ts=4:et
 */
