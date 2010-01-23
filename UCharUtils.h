/* Copyright (C) 2008 Yahoo! Inc. All Rights Reserved. */

#ifndef UCHAR_UTILS_H
#define UCHAR_UTILS_H

#include <unicode/ucnv.h>
#include <string>
#include "FwCode.h"
#include <unicode/uregex.h>

// Forward declaration
class UCharUtilsImpl;

/**
 * Some handy utilities for working with unicode characters.  Yes, these
 * could have just been some regular routines instead of static methods
 * in a class, but doing it this way gives us some containment of what
 * other static tidbits might be necessary (like reusable buffer space).
 * which are all hidden within the UCharUtilsImpl class.
 *
 * This is a singleton - do not use in a threaded program.
 */
class UCharUtils {
    private:

        /**
         * Our pointer to all sorts of goodness.
         */
        static UCharUtilsImpl *instance_;
    public:

        /**
         * Initialize the utilities.  Primarily opens the utf-8 converter.
         * Calling this is required prior to using the converter.
         * 
         * @return FwCode::FwOk on success, FwCode::UcnvOpenFailed on
         *         failure.
         */
        static FwCode::ResponseCode init();

        /**
         * Release all resources.  <code>init()</code> must be called again
         * in order to use again.
         */
        static void close();

        /**
         * Small wrapper to hide multi-line thoth api inside single-line call.
         *
         * @param value string to be tested for utf-8-ness
         * @return true if it is utf-8, false if not
         */
        static bool isUTF8(const std::string& value);

        /**
         * Small wrapper to hide multi-line thoth api inside single-line call.
         *
         * @param value char string to be tested for utf-8-ness
         * @param value_len length of <code>value</code>
         * @return true if it is utf-8, false if not
         */
        static bool isUTF8(const char * value, size_t value_len);

        /**
         * Convert utf-8 strings into UChar strings. Note that the
         * result is an internal reusable buffer so the caller should 
         * *not* release it.
         * @param input utf-8 string to convert
         * @param len set to length of output string
         * @return NULL if anything bad happens, otherwise an allocated UChar *
         *         the caller must *NEVER* free this pointer. 
         */
        static UChar * getUChar(const std::string &input, int32_t& len);

        /**
         * Do a NFC normalization so that different yet equivalent strings
         * will have a single representation. See 
         * http://www.unicode.org/unicode/reports/tr15/
         * for more information.
         * @param input A UTF-8 string that we want to normalize
         * @param result (output) the normalized UTF-8 string
         * @return FwCode::FwOk on success,
         *         FwCode::FwError on conversion failure,
         *         FwCode::InvalidData if input was not utf-8
         */
        static FwCode::ResponseCode normalize(const std::string &input, 
                                              std::string &result);

        /**
         * Compile a regular expression in a unicode-friendly way.
         *
         * @param pattern the regexp pattern to compile.  Assumed to 
         *        be utf-8.
         * @param result (output) Set to point to the compiled regexp.
         *        Must be released by the caller via uregex_close() when
         *        finished with it.
         * @return FwCode::FwOk if compilation succeeded,
         *         FwCode::CompileRegExFailed or FwCode::ConvertToUCharFailed
         *         on failure.
         */
        static FwCode::ResponseCode parseRegExpPattern
            (const std::string &pattern,
             URegularExpression * & result /* out */);

};

/**
 * Bug 2574599 - Impl exposed for use by multiple threads; singleton not
 * appropriate for multi-threaded program.
 */
class UCharUtilsImpl
{
private:
    UConverter *uconv_;

public:
    UCharUtilsImpl();
    ~UCharUtilsImpl();

    FwCode::ResponseCode init();
    void reset();
    FwCode::ResponseCode convert(const std::string &input, int32_t &len);

    FwCode::ResponseCode normalize(const std::string &nput, std::string &result);

    // Buffer used to convert from UTF-* into UChar
    int32_t ucBuffLen;
    UChar *ucBuff;

    // Buffer used for UChar normalization output
    int32_t ucNormBuffLen;
    UChar *ucNormBuff;

    // Buffer used to convert UChars back to UTF-8
    int32_t charBuffLen;
    char   *charBuff;
};

#endif // _DHT_UCHAR_UTILS_
