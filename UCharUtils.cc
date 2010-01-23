/* $Id: UCharUtils.cc,v 1.16 2009/03/03 20:19:18 dlomax Exp $ */
/* Copyright (C) 2008 Yahoo! Inc. All Rights Reserved. */

//#include <dht/UCharUtils.h>
#include "UCharUtils.h"
#include <log4cpp/Category.hh>
#include "LogUtils.h"
//#include "ActionContext.h"
#include <unicode/ucnv.h>
#include <unicode/unorm.h>
#include <thoth/validate.h> // To make sure we have UTF-8

static log4cpp::Category &log = 
                    log4cpp::Category::getInstance("dht.framework." __FILE__);


UCharUtilsImpl *UCharUtils::instance_ = NULL;

UCharUtilsImpl::
UCharUtilsImpl() : uconv_(NULL) { 
    LOG_METHOD();

    ucBuffLen = 0;
    ucBuff = NULL;

    ucNormBuffLen = 0;
    ucNormBuff = NULL;

    charBuffLen = 0;
    charBuff = NULL;
}

FwCode::ResponseCode UCharUtilsImpl::
init()
{
    UErrorCode erc = U_ZERO_ERROR;

    uconv_ = ucnv_open("utf-8", &erc);
    if (uconv_ == NULL) {
        DHT_ERROR_STREAM() << "EC:UNICODE:Problem geting utf-8 converter, erc:" << erc
                           << ", " << u_errorName(erc);
        return FwCode::UcnvOpenFailed;
    }
    return FwCode::FwOk;
}

UCharUtilsImpl::
~UCharUtilsImpl() {
    reset();
    if (uconv_ != NULL) {
        ucnv_close(uconv_);
        uconv_ = NULL;
    }
}

void UCharUtilsImpl::
reset() {
    LOG_METHOD();

    if (ucBuff != NULL) {
        delete[] ucBuff;
        ucBuffLen = 0;
        ucBuff = NULL;
    }
    if (ucNormBuff != NULL) {
        delete[] ucNormBuff;
        ucNormBuffLen = 0;
        ucNormBuff = NULL;
    }
    if (charBuff != NULL) {
        delete[] charBuff;
        charBuffLen = 0;
        charBuff = NULL;
    }
}

/**
 * Small wrapper to hide multi-line thoth api inside single-line call.
 */
bool UCharUtils::
isUTF8(const std::string& value)
{
    size_t pos = 0;
    thoth_result result = thoth_validate_utf8(value.c_str(), value.length(),
                                              &pos);
			
    if(result != UTF8_VALID) {
        std::cerr 
            //RESPONSE_DEBUG_STREAM(FwCode::DataNotUtf8)
            << "value (" << value << ") is not UTF-8. thoth_result:" << result
            << ", position=" << pos;
        return false;
    }
    return true;
}

/**
 * Small wrapper to hide multi-line thoth api inside single-line call.
 */
bool UCharUtils::
isUTF8(const char * value, size_t value_len)
{
    size_t pos = 0;
    thoth_result result = thoth_validate_utf8(value, value_len, &pos);
			
    if(result != UTF8_VALID) {
        //RESPONSE_DEBUG_STREAM(FwCode::DataNotUtf8)
        std::cerr
            << "value (" << std::string(value, value_len)
            << ") is not UTF-8. thoth_result:" << result
            << ", position=" << pos;
        return false;
    }
    return true;
}

// Convert an input string (expected to be UTF-8) into unicode UChars
// The result of the conversion will be sitting in our ucBuff area.
FwCode::ResponseCode UCharUtilsImpl::
convert(const std::string &input, int32_t &len)
{
    LOG_METHOD();

    //UTF-8 validation
    if(!UCharUtils::isUTF8(input)) {
        return FwCode::DataNotUtf8;
    }

    int size = input.length() * 2;

    // Check if we already have a big enough buffer
    if (ucBuffLen < size) {
        // Nope, first check if we need to release what we've been using
        if (ucBuff) {
            delete[] ucBuff;
        }
        ucBuffLen = size;
        ucBuff = new UChar[ucBuffLen];
    }

    UErrorCode erc = U_ZERO_ERROR;
    len = ucnv_toUChars(uconv_, 
                        ucBuff, 
                        ucBuffLen,
                        input.data(), 
                        input.length(), &erc);

    if (U_FAILURE(erc)) {
        //RESPONSE_ERROR_STREAM(FwCode::ConvertToUCharFailed)
        std::cerr
            << "EC:UNICODE:error:" << erc
                                                            << ", " << u_errorName(erc)
                           << " from converting input:'" << input << "'";
        len = 0;
        return FwCode::ConvertToUCharFailed;
    }
    return FwCode::FwOk;
}

// Normalize an input string. Note that all three internal buffers will
// be used by this operation, but by the time we finish, we'll be done
// with them.
FwCode::ResponseCode UCharUtilsImpl::
normalize(const std::string &input, std::string &result /* out */)
{
    LOG_METHOD();

    // convert our UTF-8 into UChar
    int32_t inLen = 0;
    FwCode::ResponseCode rc = convert(input, inLen);

    if (rc != FwCode::FwOk) {
        result.erase();
        return rc;
    }

    // Do a quick check if the input is already normalized so that
    // we can duck out early
    UErrorCode status = U_ZERO_ERROR;
    if (unorm_quickCheck(ucBuff, inLen,
                         UNORM_NFC, &status) == UNORM_YES) {
        DHT_DEBUG_STREAM() << "already normalized input:" << input;
        result = input;
        return FwCode::FwOk;
    }

    // Check if we have enough space for the normalized result.
    // We'll make the output space twice as big as the input (although
    // it's more likely that the normalized result will be shorter
    // as it combines characters. E.g. 'A' 'put an accent on the previous'
    int32_t newSize = inLen * 2;
    if (newSize > ucNormBuffLen) {
        DHT_DEBUG_STREAM() << "newSize:" << newSize
                           << " ucNormBuffLen:" << ucNormBuffLen;
        if (ucNormBuff) {
            delete[] ucNormBuff;
        }
        ucNormBuffLen = newSize;
        ucNormBuff = new UChar[ucNormBuffLen];
    }

    // Do the actual normalization
    status = U_ZERO_ERROR;
    int32_t normLen = unorm_normalize(ucBuff, inLen,
                                                        UNORM_NFC, 0,
                                                        ucNormBuff, 
                                                        ucNormBuffLen,
                                                        &status);
    if(U_FAILURE(status)) {
        //RESPONSE_ERROR_STREAM(FwCode::FwError)
        std::cerr
            << "EC:UNICODE:error:" << status << ", " << u_errorName(status)
                           <<" in unorm_normalize, inLen:" << inLen
                           << " ucNormBuffLen:" << ucNormBuffLen;
        return FwCode::FwError;
    }

    // Make sure we have some space to convert back to UTF-8
    int32_t resultLen = normLen * 4;
    if (resultLen > charBuffLen) {
        DHT_DEBUG_STREAM() << "resultLen:" << resultLen
                           << " charBuffLen:" << charBuffLen;
        if (charBuff) {
            delete[] charBuff;
            charBuff= NULL;
        }
        charBuffLen = resultLen;
        charBuff = new char[charBuffLen];
    }

    DHT_DEBUG_STREAM() <<"calling ucnv_fromUChars, normLen:" << normLen;

    // Go from UChar array to UTF-8
    int32_t actualLen = ucnv_fromUChars(uconv_,
                                                          charBuff, charBuffLen,
                                                          ucNormBuff, normLen,
                                                          &status);
    if(U_FAILURE(status)) {
        //RESPONSE_ERROR_STREAM(FwCode::FwError)
        std::cerr
            << "EC:UNICODE:error:" << status << ", " << u_errorName(status)
                           << " in ucnv_fromUChars charBuffLen:" << charBuffLen
                           << " normLen:" << normLen;
        return FwCode::FwError;
    }

    // Smack our UTF-8 characters into the result string
    result.assign(charBuff, actualLen);
    DHT_DEBUG_STREAM() << "leaving actualLen:" << actualLen
                       << " result:" << result;
    return FwCode::FwOk;
}


FwCode::ResponseCode UCharUtils::
init()
{
    if (instance_ == NULL) {
        instance_ = new UCharUtilsImpl();
        return instance_->init();
    }
    return FwCode::FwOk;  // already initialized
}

void UCharUtils::
close()
{
    if(instance_ != NULL) {
        delete instance_;
        instance_ = NULL;
    }
}

// Given an input string, return a unicode UChar array. Note that the 
// return value is a pointer to our internal buffer.
UChar * UCharUtils::
getUChar(const std::string &input, int32_t& len) {
    LOG_METHOD();

    // do the conversion...somehow need 2x input len for utf8 to utf16
    if(instance_->convert(input, len) != FwCode::FwOk) {
        len = 0;
        return NULL;
    }

    return instance_->ucBuff;
}

FwCode::ResponseCode UCharUtils::
normalize(const std::string &input, std::string &result) {
    LOG_METHOD();
    return(instance_->normalize(input, result));
}


FwCode::ResponseCode UCharUtils::
parseRegExpPattern(const std::string &pattern,
                   URegularExpression * & result /* out */)
{
    UParseError perr;
    UErrorCode erc = U_ZERO_ERROR;
    int32_t ureglen = 0;

    // Do not delete uregexp, it's a static reusable buffer inside UCharUtils
    UChar *uregexp = UCharUtils::getUChar(pattern, ureglen);
    if (uregexp == NULL) {
        //RESPONSE_ERROR_STREAM(FwCode::ConvertToUCharFailed)
        std::cerr
            << "EC:UNICODE|IMPOSSIBLE:Unable to convert pattern to unicode: " << pattern;
        return FwCode::ConvertToUCharFailed;
    }

    URegularExpression *regexp= uregex_open(uregexp, ureglen, 0, 
                                            &perr, 
                                            &erc);
    if(erc != U_ZERO_ERROR) {
        //RESPONSE_DEBUG_STREAM(FwCode::CompileRegExFailed)
        std::cerr
            << "Compiling regex failed at: " << perr.offset
            << "; re=" << pattern;
        return FwCode::CompileRegExFailed;
    }
    
    result = regexp;
    return FwCode::FwOk;
}
