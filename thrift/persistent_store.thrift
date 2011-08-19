/*
 * Defines Sherpa persistent store interface. 
 * 
 * For a good tutorial on how to write a .thrift file, see:
 *
 * http://wiki.apache.org/thrift/Tutorial
 */
namespace cpp sherpa
namespace java com.yahoo.sherpa

enum ResponseCode 
{
    Ok = 0,
    Error,
    DatabaseExists,
    DatabaseNotFound,
    RecordExists,
    RecordNotFound,
    ScanEnded,
}

enum ScanOrder 
{
    Ascending,
    Descending,
}

struct Record 
{
    1:binary key,
    2:binary value,
}

struct RecordListResponse 
{
    1:ResponseCode responseCode,
    2:list<Record> records,
}

struct BinaryResponse 
{
    1:ResponseCode responseCode,
    2:binary value,
}

struct StringListResponse 
{
    1:ResponseCode responseCode,
    2:list<string> values,
}

/**
 * Note about database name:
 * Thrift string type translates to std::string in C++ and String in 
 * Java. Thrift does not validate whether the string is in utf8 in C++,
 * but it does in Java. If you are using this class in C++, you need to
 * make sure the database name is in utf8. Otherwise requests will fail.
 */
service PersistentStore 
{
    /**
     * Pings this persistent store.
     *
     * @return Ok - if ping was successful.
     *         Error - if ping failed.
     */
    ResponseCode ping(),

    /**
     * Cleanly shuts down the persistent store.
     *
     * @return Ok - if shutdown is in progress.
     *         Error - otherwise
     */
    ResponseCode shutdown(),

    /**
     * Add a new database to this persistent store.
     *
     * A database is a container for a collection of records.
     * A record is a binary key / binary value pair. 
     * A key uniquely identifies a record in a database. 
     * 
     * @param databaseName database name
     * @return Ok - on success.
     *         DatabaseExists - database already exists.
     *         Error - on any other errors.
     */
    ResponseCode addDatabase(1:string databaseName),

    /**
     * Drops a database from this persistent store.
     *
     * @param databaseName database name
     * @return Ok - on success.
     *         DatabaseNotFound - database doesn't exist.
     *         Error - on any other errors.
     */
    ResponseCode dropDatabase(1:string databaseName),

    /**
     * List databases in this persistent store.
     *
     * @returns StringListResponse
     *              responseCode Ok - on success.
     *                           Error - on error.
     *              values - list of databases.
     */
    StringListResponse listDatabases(),

    /**
     * Returns records in a database in lexicographical order.
     *
     * Note that startKey is supposed to be smaller than or equal to the endKey
     * regardress of the scan order. For example, to scan all the records from
     * "apple" to "banana" in descending order, startKey is "apple" and endKey
     * is "banana". If startKey is larger than endKey, scan will succeed and 
     * result will be empty.
     *
     * This method will return ScanEnded if the scan was successful and it reached
     * the end of the key range. It'll return Ok if it reached maxRecords or 
     * maxBytes, but it didn't reach the end of the key range. 
     *
     * @param databaseName database name
     * @param order Ascending or Decending.
     * @param startKey Key to start scan from. If it's empty, scan starts 
     *                 from the smallest key in the database.
     * @param startKeyIncluded
     *                 Indicates whether the record that matches startKey is
     *                 included in the response.
     * @param endKey   Key to end scan at. If it's emty scan ends at the largest
     *                 key in the database.
     * @param endKeyIncluded
     *                 Indicates whether the record that matches endKey is
     *                 included in the response.
     * @param maxRecords 
     *                 Scan will return at most $maxRecords records.
     * @param maxBytes Advise scan to return at most $maxBytes bytes. This 
     *                 method is not required to strictly keep the response
     *                 size less than $maxBytes bytes. 
     * @return RecordListResponse
     *             responseCode - Ok if the scan was successful
     *                          - ScanEnded if the scan was successful and 
     *                                      scan reached the end of the range. 
     *                          - DatabaseNotFound database doesn't exist.
     *                          - Error on any other errors
     *             records - list of records. 
     */
    RecordListResponse scan(1:string databaseName,
                            2:ScanOrder order,
                            3:binary startKey,
                            4:bool startKeyIncluded,
                            5:binary endKey,
                            6:bool endKeyIncluded,
                            7:i32 maxRecords,
                            8:i32 maxBytes),

    /**
     * Retrieves a record from a database.
     *
     * @param databaseName database name
     * @param recordKey record to retrive.
     * @returns RecordListResponse
     *              responseCode - Ok 
     *                             DatabaseNotFound database doesn't exist.
     *                             RecordNotFound record doesn't exist.
     *                             Error on any other errors.
     *              records - list of records
     */
    BinaryResponse get(1:string databaseName, 2:binary recordKey),

    /**
     * Inserts a record into a database.
     *
     * @param databaseName database name
     * @param recordKey record key to insert
     * @param recordValue  record value to insert
     * @returns Ok 
     *          DatabaseNotFound database doesn't exist.
     *          RecordExists
     *          Error
     */
    ResponseCode insert(1:string databaseName, 2:binary recordKey, 3:binary recordValue),

    /**
     * Inserts multiple records into a database.
     *
     * This operation is atomic: either all the records get inserted into a database
     * or none does. 
     * 
     * @param databaseName database name
     * @param records list of records to insert
     * @returns Ok
     *          DatabaseNotFound
     *          RecordExists - if a record already exists in a database
     *          Error
     */
    ResponseCode insertMany(1:string databaseName, 2:list<Record> records),

    /**
     * Updates a record in a database.
     *
     * @param databaseName database name
     * @param recordKey record key to update
     * @param recordValue new value for the record
     * @returns Ok 
     *          DatabaseNotFound database doesn't exist.
     *          RecordNotFound
     *          Error
     */
    ResponseCode update(1:string databaseName, 2:binary recordKey, 3:binary recordValue),

    /**
     * Removes a record from a database.
     *
     * @param databaseName database name
     * @param recordKey record to remove from the database.
     * @returns Ok 
     *          DatabaseNotFound database doesn't exist.
     *          RecordNotFound
     *          Error
     */
    ResponseCode remove(1:string databaseName, 2:binary recordKey),
}
