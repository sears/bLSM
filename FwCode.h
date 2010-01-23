/* Copyright (C) 2008 Yahoo! Inc. All Rights Reserved. */

#ifndef __FW_CODE__H
#define __FW_CODE__H

#include <string>

/**
 * Global framework response codes.
 */
class FwCode {
 public:

    typedef int ResponseCode;

    static const std::string unknownCodeStr;

    /**
     * The convention here is to keep related codes grouped together, so
     * that it is easier to find all existing codes for a particular
     * module.  Each section is given a range of 50 codes, so that adding
     * a new code to an existing section won't invalidate all of the codes
     * following it in the enum (causing binary incompatibility).
     */

    //----------- Generic section -------------
    static const ResponseCode FwOk = 0;    //!< All successes
    static const ResponseCode FwError = 1; //!< General error code

    static const ResponseCode FwCrit = 2;  //!< General critical error. could be originated by low level library to indicate some nasty error has occurred.

    static const ResponseCode MdbmOpenFailed = 3; //!< Any kind of mdbm open failure
    static const ResponseCode MdbmOperationFailed = 4; //!< Any store/fetch/lock from mdbm failed
    static const ResponseCode NoMem = 5; //!< Out Of Memory
    static const ResponseCode InvalidParam = 6; //!< Invalid parameter
    static const ResponseCode NotFound = 7; //!< Fail to find the specified info; usuall returned by access methods
    static const ResponseCode InvalidState = 8; //!< Invalid state
    static const ResponseCode ConnReset = 9; //!< connection reset
    static const ResponseCode Timeout = 10; //!< operation timed out
    static const ResponseCode InvalidData = 11; //!< buffer data is invalid
    static const ResponseCode BufTooSmall = 12;  //!< Buffer size is smaller than required
    static const ResponseCode MalformedRequest = 13; //!< Request data (like the URI) is malformed
    static const ResponseCode RequestTooLarge = 14; //!< Request data (like the body) is too big
    static const ResponseCode ConvertToDhtDataFailed = 15; // !< Failed convert json string to DHT::Data
    static const ResponseCode ConvertFromDhtDataFailed = 16; // !< Failed to convert DHT::Data to json string
    static const ResponseCode BadHexString = 17; //!< Failed to parse a hex string
    static const ResponseCode ShmemCorrupted = 18;  //!< A shared mem corruption has been detected.
    static const ResponseCode ParseError = 19; //!< Generic parsing problem
    /// If mdbm unlock fails, most of the time we want to shut off the
    /// system automatically, without letting the caller know that we did
    /// so. On specific instances where the caller is the FaultHandler, or
    /// Oversight Fault counter (there may be other examples), we don't want
    /// to do this because we want to avoid cross-dependency.
    static const ResponseCode MdbmUnlockFailed = 20;

    //----------- Generic section -------------
    // Config
    static const ResponseCode ConfigFailure = 50;  //!< Failure to find or parse a config entry

    //----------- UChar section -------------
    // UCharUtils
    static const ResponseCode UcnvOpenFailed = 100; //!< Failed to open ucnv converter for utf-8
    static const ResponseCode DataNotUtf8 = 101;    //!< Data is not in utf-8 format
    static const ResponseCode ConvertToUCharFailed = 102; //!< Failed to convert utf-8 string to UChar string
    static const ResponseCode CompileRegExFailed = 103; //!< Failed to compile the regular expression

    //----------- Yca section -------------
    // YcaClient
    static const ResponseCode YcaOpenFailed = 150; //!< Failed to open the yca database
    static const ResponseCode YcaCertInvalid = 151; //!< Validation of presented cert failed
    static const ResponseCode YcaCertNotFound = 152;        //!< certificate for the requested appID was not found

    //----------- Broker section -------------
    static const ResponseCode BrokerClientOpenFailed = 200;  //!< Failed to connect to broker
    static const ResponseCode UncertainPublish = 201; //!< Publish was uncertain - unknown if it happened
    static const ResponseCode PublishFailed = 202;   //!< Publish failed (for certain :))
    static const ResponseCode SubscribeFailed = 203; //!< Failed to subscribe to a topic
    static const ResponseCode NoSubscriptionFound = 204; //!< Operation on a sub failed because we (locally)
    // don't know about it
    static const ResponseCode RegisterFailed = 205; //!< Failed to register handler for subscription
    static const ResponseCode UnsubscribeFailed = 206; //!< Failed to unsubscribe from sub
    static const ResponseCode ListTopicsFailed = 207; //!< Failed to list subscribed topics
    static const ResponseCode ConsumeFailed = 208; //!< Failed to consume messages for a topic
    static const ResponseCode TopicInvalid = 209;  //!< Topic is invalid (was usurped or ymb 'lost' it)
    static const ResponseCode NoMessageDelivered = 210;  //!< Call to deliver() found no messages ready
    static const ResponseCode ConsumeFailedBadTopic = 211; //!< The topic is bad - our handle is bad,
    // or it got usurped
    static const ResponseCode ConsumeFailedBadHandle = 212; //!< Our ymb handle is bad - not usable anymore
    static const ResponseCode ConsumeFailedConnectionError = 213; //!< a recoverable connection error
    static const ResponseCode ConsumeFailedServerBusy = 214; //!< ymb server is having a temporary issue,
    // not a failure per se
    // second argument to messageProcessed()
    static const ResponseCode ConsumeMessage = 215; //!< consume this message
    static const ResponseCode ConsumeAndUnsubscribe = 216; //!< end this channel
    // Internal to ymb implementation
    static const ResponseCode YmbSubscribeTempFailure = 217;  //!< A failure that might be resolved on a retry
    static const ResponseCode YmbSubscribeTimedout = 218; //!< A timeout failure
    static const ResponseCode YmbSubscriptionExists = 219; //!< Attempt to create a sub that already exists
    static const ResponseCode NoSuchSubscription = 220; //!< Attempt to attach to a sub that does not exist
    static const ResponseCode AttachNoSuchSubscription = 221; //!< Specific to attach, no subscription to attach to (not necessarily an error)
    static const ResponseCode BrokerInitFailed = 222; //!< Config or allocation failed
    static const ResponseCode BrokerConnectionLost = 223; //!< Lost connection to broker
    static const ResponseCode BrokerFatalError = 224; //!< Generally shared mem corruption


    //----------- Daemon section -------------
    // Daemon
    static const ResponseCode NoImpl = 250;    //!< No op
    static const ResponseCode Restart = 251; //!< Exit the daemon so that it is restarted right away.
    // request that the daemon do a soft restart
    static const ResponseCode Exit = 252; //!< Exit the daemon so that it is NOT restarted right away. A monitoring process may restart the entire system later.
    static const ResponseCode StopDelivery = 253; //!< Stop delivery on the topic, returned by Broker handlers only.
    static const ResponseCode RetryDelivery = 254; //!< Stop delivery on the topic but retry after sometime, returned by Broker handlers only.

    //----------- Lock section -------------
    // LockManager
    //ALL these lock errors are handled in SuFaulHandler.cc
    //Any addition to these error codes requires update to the SuFaultHandler
    static const ResponseCode LockSyserr = 301;        //!< System error during lock/unlock op
    static const ResponseCode LockInconsis = 302;        //!< Inconsistency detected in LockManager.
    static const ResponseCode LockNested = 303;         //!< Nested locking of same key not allowed.
    static const ResponseCode LockNosuchpid = 304;      //!< This pid does not hold the lock.
    static const ResponseCode LockUnavail = 305;        //!< Outa lock
    static const ResponseCode LockInitfail = 306;        //!< Initialization failure of the lock subsystem
    static const ResponseCode LockInvalidarg = 307;      //!< Invalid arguments to lock subsystem.

    //----------- Message section -------------
    //Message and Message serialization
    static const ResponseCode SerializeFailed = 350;     //!< Message Serialization Failed
    static const ResponseCode DeserializeFailed = 351;   //!< Message Deserialization failed
    static const ResponseCode NoResponseCodeInMessage = 352;

    //----------- Transport Errors -------------
    static const ResponseCode TransportSendError = 400;    //!< Curl error in communicating with other server
    static const ResponseCode TransportSetHeaderFailed = 401; //!< Error in setting header in curl request
    static const ResponseCode TransportCurlInitError = 402;  // !< Error initializing curl handle -- should be curl specific
    static const ResponseCode TransportUncertain = 403;  //!< Send came back uncertain (timeout, usually)
    static const ResponseCode TransportInvalidResponseBody = 404;  //!< Send came back unparsable body

    //----------- Apache/Web section -------------
    static const ResponseCode EndOfBody = 450;    //!< Normal end of incoming request body
    static const ResponseCode BodyReadFailed = 451;    //!< Failed reading incoming request body
    static const ResponseCode BodyWriteFailed = 452;    //!< Failed writing outgoing request body
    static const ResponseCode EncryptionFailed = 453;    //!< Failed to encrypt body or header
    static const ResponseCode DecryptionFailed = 454;    //!< Failed to decrypt body or header
        
    /**
     * Give back a basic, generic string description of the response code.
     *
     * @param rc The response code to convert.
     * @return The string describing it.
     */
    static std::string toString(ResponseCode rc);

};

/* For customized vim control
 * Local variables:
 * tab-width: 4
 * c-basic-offset: 4
 * End:
 * vim600: sw=4:ts=4:et
 * vim<600: sw=4:ts=4:et
 */
#endif
