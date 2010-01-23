/* $Id: StringUtils.h,v 1.17 2009/03/25 20:32:51 dlomax Exp $ */
/* Copyright (C) 2008 Yahoo! Inc. All Rights Reserved. */

#ifndef __STRING_UTIL_H
#define __STRING_UTIL_H
#include <iostream>
#include <iomanip>
#include <sstream>
#include "FwCode.h"

/**
 * Container for static string manipulation utilities.
 */
class StringUtils
{
 public:

    /**
     * Our replacement for yax_getroot().  Allows our code to have a different
     * root than components we use or link with.  Is nice for unit testing.
     * @return Copy of the value in a std::string
     */
    static std::string getDhtRoot();

    /**
     * Parse a tablet name into left and right limits.
     * @return true if parsing successful, false if incorrect format
     */
    static bool parseTabletName(const std::string& tablet, std::string& leftLimit,
                                std::string& rightLimit);

    /**
     * Construct a tablet name from left and right limits.
     */
    static void buildTabletName(const std::string& leftLimit,
                                const std::string& rightLimit,
                                std::string& tablet);
 
    /**
     * General purpose method to assemble a full path name, using
     * getDhtRoot() so that
     * the root will be configurable.  DO NOT supply "/home/y" in path1.
     */
    static std::string makePath(const std::string& path1 = "",
                                const std::string& path2 = "",
                                const std::string& path3 = "",
                                const std::string& path4 = "",
                                const std::string& path5 = "",
                                const std::string& path6 = "");

    /**
     * Append additional paths to an existing one - does not prepend ROOT.
     */
    static void appendPath(std::string& base_path, const std::string& path2 = "",
                            const std::string& path3 = "",
                            const std::string& path4 = "");

    /**
     * Construct a topic name from a table/tablet.
     * 
     * @return the topic name
     */
    static std::string buildTopicName(const std::string& table,
                                      const std::string& tablet);

    /**
     * Construct a topic name from a table/tablet.
     * @param topic  Is filled with the topic name.
     */
    static void buildTopicName(const std::string& table,
                               const std::string& tablet,
                               std::string &topic);

    /**
     * Parses <code>topic</code> into table and tablet portions.
     *
     * @param table Filled with the table name.
     * @param tablet Filled with the tablet name.
     * @param true if the parsing succeeded, false if not.
     */
    static bool parseTopicName(const std::string& topic,
                               std::string& table,
                               std::string &tablet);

    /**
     * Only for use in log statements - this is slow.  Produce a printable
     * string where binary (<32) characters are hex encoded, but all others
     * are left alone.
     *
     * @param str string to encode
     * @param len length of string
     * @return encoded string.
     */
    static std::string toPrintable(const char *str, size_t len);

    /**
     * Convert a formatted hex string back into its original
     * 64-bit value
     *
     * @param value the hex-encoded string
     * @param out the value
     * @return FwCode::FwOk on success, FwCode::BadHexString on parse failure
     */
    static FwCode::ResponseCode
        convertHexStringToUI64(const std::string& value, uint64_t& out);

    /**
     * Convert a formatted hex string back into its original
     * 32-bit value
     *
     * @param value the hex-encoded string
     * @param out the value
     * @return FwCode::FwOk on success, FwCode::BadHexString on parse failure
     */
    static FwCode::ResponseCode 
        convertHexStringToUI32(const std::string& value, uint32_t& out);

    /**
     * Standard means for formatting a 0x prefixed hex string from a
     * 64-bit unsigned value.  Will produce upper-case letters.  Will
     * pad with zeros at the beginning to fill out 16 hex chars.
     *
     * @param the value to format
     * @return the formatted value, like "0xDEADBEEF00000000"
     */
    static std::string convertUI64ToHexString( uint64_t val );

    /**
     * Standard means for formatting a 0x prefixed hex string from a
     * 32-bit unsigned value.  Will produce upper-case letters.  Will
     * pad with zeros at the beginning to fill out 8 hex chars.
     *
     * @param the value to format
     * @return the formatted value, like "0xDEADBEEF"
     */
    static std::string convertUI32ToHexString( unsigned int val );

    /**
     * Standard means for formatting a small hex string from a
     * 32-bit unsigned value.  The "0x" will NOT be included.
     * Will produce upper-case letters.  Will NOT pad with zeros
     * at the beginning.
     *
     * @param the value to format
     * @return the formatted value, like "DEADBEEF"
     */
    static std::string convertUI32ToMinimalHexString( unsigned int val );

    /**
     * Assemble the fields of ENCRYPTED_BODY_HEADER and encrypt it for
     * sending to the remote side.
     * @param result is the out parameter having the resulting string.
     * @param encKeyName is the name of the key in keydb whose value will be
     * used as the encryption key
     * @param bodyEncVersion is the version of the encryption scheme used to
     * encrypt the body (not the encryption scheme of this header itself).
     * @param expireTime is the time (in usecs) after which the request
     * should not be processed by the receiver of this header.
     */
    static FwCode::ResponseCode makeEncryptedBodyHdr(std::string & result,
            const char *encKeyName, uint32_t bodyEncVersion, uint64_t expireTime);

    /**
     * Parse the incoming ENCRYPTED_BODY_HEADER, decrypting it, and
     * separating the fields in it.
     * @param inval is the incoming encrypted string.
     * @param encKeyName is the name of the key in keydb whose value will be
     * used as the decryption key
     * @param bodyEncVersion is the version of the encryption scheme to be
     * used to * decrypt the body (not for the decryption of this header
     * itself).
     * @param expireTime is the time (in usecs) after which the response
     * should not be processed by the receiver of this header.
     */
    static FwCode::ResponseCode parseEncryptedBodyHdr(const std::string & inval,
            const char *encKeyName, uint32_t & bodyEncVersion, uint64_t & expireTime);

    /**
     * Get the hash for an un-normalized record name.
     *
     * @param unnormalizedRecordName a raw record name from user input
     * @param (output) hashResult the hex string of the hash value.
     * @return FwCode::FwOk on success, else an error relating to normalization
     */
    static FwCode::ResponseCode normalizeAndHashRecordName
        ( const std::string& unnormalizedRecordName,
          std::string & hashResult /* out */ );

    /**
     * Get the hash for a normalized record name.
     *
     * @param recordName the record name.  MUST be previously normalized.
     * @return hashResult the uint32_t of the hash value.
     */
    static uint32_t hashRecordName(const std::string& recordName);

    /**
     * Get the hash for a normalized record name.
     *
     * @param recordName the record name.  MUST be previously normalized.
     * @param (output) hashResult the hex string of the hash value.
     */
    static void hashRecordName( const std::string& recordName,
                                std::string & hashResult /* out */ );
    /**
     * Get the hash for a normalized record name in string and int form
     *
     * @param recordName the record name.  MUST be previously normalized.
     * @param (output) hashResult the hex string of the hash value.
     * @param (output) hexNum numerical value of hash
     */
    static void hashRecordName( const std::string& recordName,
                                std::string & hashResult /* out */,
                                uint32_t& hexNum);

    /**
     * Method to hash a string using crc32.
     *
     * @param buf data to hash
     * @param len length of buf
     * @return hash value
     */
    static uint32_t crcHash(const char * buf, uint32_t len);

    /**
     * util function to convert any type to a string
     */
    template<typename T> static inline std::string toString(T item);
    
    /** 
     * convert string to any type of value
     * @param strValue string value to parse
     * @param value(out) value to read from strValue
     * @return FwCode::FwOk on success
     *         FwCode::FwError on failure (error is *not* logged)
     */
    template<typename T> static inline 
    FwCode::ResponseCode  fromString(const std::string& strValue,
                                     T& value);

    /** 
     * convert a hexadecimal number to string representation 
     * of fixed width ( 2 * sizeof(T) )
     * @param value number to convert to string
     * @return string representation of value
     */
    template<typename T> static inline
    std::string numberToHexString(T value);

    /** 
     * convert a hexadecimal number to minimal string representation
     * @param value number to convert to string
     * @return string representation of value
     */
    template<typename T> static inline
    std::string numberToMinimalHexString(T value);
    
    /**
     * convert a hexadecimal string to a number
     * @param strvalue input string to read from
     * @param value(out) output number
     * @return FwCode::FwOk on successful conversion
     *         FwCode::FwError on failure to convert strvalue
     *         to number
     */
    template<typename T> static inline 
    FwCode::ResponseCode hexStringToNumber(const std::string& strvalue,
                                           T& value);

    
    static const std::string EMPTY_STRING;
};

template<typename T> 
std::string StringUtils::
toString(T item) 
{
    std::ostringstream buf;
    buf << item;
    return buf.str();
}

template<typename T>
FwCode::ResponseCode  StringUtils::
fromString(const std::string& strValue,
           T& value)
{
    std::istringstream buf(strValue);
    buf >> value;
    if(buf.fail()|| 
       (strValue.length() != buf.tellg() )) 
    {
        return FwCode::FwError;
    }
    return FwCode::FwOk;
}

template<typename T>
std::string StringUtils::
numberToHexString(T value)
{
    std::ostringstream buf;
    buf << "0x" << std::hex 
        << std::setw(sizeof(T) * 2) << std::setfill('0') 
        << std::uppercase << value;
    return buf.str();

}

template<typename T>
std::string StringUtils::
numberToMinimalHexString(T value)
{
    std::ostringstream buf;
    buf << std::hex << std::uppercase << value;
    return buf.str();

}

template<typename T>
FwCode::ResponseCode StringUtils::
hexStringToNumber(const std::string& strvalue,
                  T& value)
{
    std::istringstream buf(strvalue);
    buf >> std::hex >> value;
    if(buf.fail() || 
       (strvalue.length() != buf.tellg() )) 
    {
        return FwCode::FwError;
    }
    return FwCode::FwOk;

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
#endif
