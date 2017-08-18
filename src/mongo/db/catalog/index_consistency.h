/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/db/storage/sorted_data_interface.h"
#include "mongo/util/elapsed_tracker.h"

namespace mongo {

/**
 * The ValidationStage allows the IndexConsistency class to perform
 * the correct operations that depend on where we are in the validation.
 */
enum class ValidationStage { DOCUMENT, INDEX, NONE };

/**
 * The ValidationOperation is used by classes using the IndexObserver to let us know what operation
 * was associated with it.
 * The `UPDATE` operation can be seen as two independent operations (`REMOVE` operation followed
 * by an `INSERT` operation).
 */
enum class ValidationOperation { INSERT, REMOVE, NONE };

/**
 * The IndexConsistency class is used to keep track of the index consistency.
 * It does this by using the index keys from index entries and index keys generated from the
 * document to ensure there is a one-to-one mapping for each key.
 * In addition, an IndexObserver class can be hooked into the IndexAccessMethod to inform
 * this class about changes to the indexes during a validation and compensate for them.
 */

/**
 * Contains all the index information and stats throughout the validation.
 */
struct IndexInfo {
    // Informs us if the index was ready or not for consumption during the start of validation.
    bool isReady;
    // Contains the pre-computed hashed of the index namespace.
    uint32_t indexNsHash;
    // True if the index has finished scanning from the index scan stage, otherwise false.
    bool indexScanFinished;
    // The number of index entries belonging to the index.
    int64_t numKeys;
    // The number of long keys that are not indexed for the index.
    int64_t numLongKeys;
    // The number of records that have a key in their document that referenced back to the
    // this index
    int64_t numRecordKeys;
};

class IndexConsistency final {
public:
    IndexConsistency(OperationContext* opCtx,
                     Collection* collection,
                     NamespaceString nss,
                     RecordStore* recordStore,
                     std::unique_ptr<Lock::CollectionLock> collLk,
                     const bool background);

    /**
     * Helper functions for `_addDocKey`, `_removeDocKey`, `_addIndexKey`,
     * and `_removeIndexKey` for concurrency control.
     */
    void addDocKey(const KeyString& ks, int indexNumber);
    void removeDocKey(const KeyString& ks, int indexNumber);
    void addIndexKey(const KeyString& ks, int indexNumber);
    void removeIndexKey(const KeyString& ks, int indexNumber);

    /**
     * Add one to the `_longKeys` count for the given `indexNs`.
     * This is required because index keys > `KeyString::kMaxKeyBytes` are not indexed.
     */
    void addLongIndexKey(int indexNumber);

    /**
     * Returns the number of index entries for the given `indexNs`.
     */
    int64_t getNumKeys(int indexNumber) const;

    /**
     * Returns the number of long keys that were not indexed for the given `indexNs`.
     */
    int64_t getNumLongKeys(int indexNumber) const;

    /**
     * Return the number of records with keys for the given `indexNs`.
     */
    int64_t getNumRecordKeys(int indexNumber) const;

    /**
     * Return the number of inserted and removed records before the cursor that happened
     * during the collection scan.
     */
    int64_t getNumRecordChangesBeforeCursor(int64_t* totalDataSize);

    /**
     * Processes a record that was inserted or removed before the cursor during
     * the collection scan.
     */
    void processRecordBeforeCursor(const RecordId& loc, int size, bool increment);

    /**
     * Returns true if any value in the `_indexKeyCount` map is not equal to 0, otherwise
     * return false.
     */
    bool haveEntryMismatch() const;

    /**
     * This is the entry point for the IndexObserver to apply its observed changes
     * while it is listening for changes in the IndexAccessMethod.
     *
     * This method ensures that during the collection scan stage, inserted, removed and
     * updated documents are reflected in the index key counts.
     * It does this by:
     * 1) Setting the yield point for the collection scan to inform us when we should
     *    get a new snapshot so we won't scan stale records.
     * 2) Calling the appropriate `addDocKey` and `removeDocKey` functions if the
     *    record comes before or equal to our last processed RecordId.
     *
     * The IndexObserver will call this method while it is observing changes during
     * the index scan stage of the collection validation. It ensures we maintain
     * a pre-image of the indexes since we established the point of validity, which
     * was determined when the collection scan stage completed.
     * It does this by:
     * 1) Setting the yield point for the index scan to inform us when we should get
     *    a new snapshot so we won't scan stale index entries. The difference between
     *    this and the collection scan is that it will only set the yield point for the
     *    index that is currently being scanned, since when we start the next index, we
     *    will yield before we begin and we would have the latest snapshot.
     * 2) Calling the appropriate `addIndexKey` and `removeIndexKey` functions for indexes
     *    that haven't started scanning and are not finished, or they are scanning the
     *    index and the index changes are after the last processed index entry.
     * 3) In addition, we maintain the number of external index changes here so that
     *    after we finish the index scan, we can remove the extra number of operations
     *    that happened after the point of validity.
     */
    void applyChange(const IndexDescriptor* descriptor,
                     const boost::optional<IndexKeyEntry>& indexEntry,
                     ValidationOperation operation);

    /**
     * Moves the `_stage` variable to the next corresponding stage in the following order:
     * `DOCUMENT` -> `INDEX`
     * `INDEX` -> `NONE`
     * `NONE` -> `NONE`
     */
    void nextStage();

    /**
     * Returns the `_stage` that the validation is on.
     */
    ValidationStage getStage() const;

    /**
     * Informs the IndexConsistency instance that the index scan is beginning to scan the index
     * with namespace `indexNs`. This gives us a chance to clean up after the previous index and
     * setup for the new index.
     */
    void notifyStartIndex(int indexNumber);

    /**
     * Informs the IndexConsistency instance that the index scan has finished scanning the index
     * with namespace `indexNs`. This allows us to clean up just like in `notifyStartIndex` and to
     * set the index to a finished state so that the hooks are prevented from affecting it.
     */
    void notifyDoneIndex(int indexNumber);

    /**
     * Returns the index number for the corresponding index namespace's.
     */
    int getIndexNumber(const std::string& indexNs);

    /**
     * Returns false if a new snapshot should be accquired.
     * If the `recordId` is equal to or greater than `_yieldAtRecordId` then
     * a new snapshot must be acquired otherwise stale data will be used.
     *
     * If a new snapshot doesn't need to be acquired, the `_lastProcessedRecordId` is set to
     * `recordId` and returns true, informing that the caller should proceed their cursor.
     */
    bool shouldGetNext(const RecordId recordId);

    /**
     * Returns false if a new snapshot should be accquired.
     * If the `keyString` is equal to or greater than `_yieldAtIndexEntry` then
     * a new snapshot must be acquired otherwise stale data will be used.
     *
     * If a new snapshot doesn't need to be acquired, the `_lastProcessedIndexEntry` is set to
     * `keyString` and returns true, informing that the caller should proceed their cursor.
     */
    bool shouldGetNext(const KeyString& keyString);

    ValidationOperation getYieldOperation();

    /**
     * Gives up the lock that the collection is currently held in and requests the
     * the collection again in LockMode `mode`
     */
    void relockCollectionWithMode(LockMode mode);

    /**
     * Returns true if the ElapsedTracker says its time to yield during background validation.
     */
    bool scanLimitHit();

    /**
     * Yields the locks temporarily to allow Global/DB/Collection operations
     * to perform their work that they couldn't while we held our lock.
     */
    void yield();

    /**
     * Stops the validation by throwing a uassert
     */
    void stop();

private:
    OperationContext* _opCtx;
    Collection* _collection;
    const NamespaceString _nss;
    const RecordStore* _recordStore;
    std::unique_ptr<Lock::CollectionLock> _collLk;
    const bool _isBackground;
    OptionalCollectionUUID _uuid;
    ElapsedTracker _tracker;

    // We map the hashed KeyString values to a bucket which contain the count of how many
    // index keys and document keys we've seen in each bucket.
    // Count rules:
    //     - If the count is 0 in the bucket, we have index consistency for
    //       KeyStrings that mapped to it
    //     - If the count is > 0 in the bucket at the end of the validation pass, then there
    //       are too few index entries.
    //     - If the count is < 0 in the bucket at the end of the validation pass, then there
    //       are too many index entries.
    std::map<uint32_t, int64_t> _indexKeyCount;

    // Contains the corresponding index number for each index namespace
    std::map<std::string, int> _indexNumber;

    // A mapping of index numbers to IndexInfo
    std::map<int, IndexInfo> _indexesInfo;

    // RecordId of the last processed document during the collection scan.
    boost::optional<RecordId> _lastProcessedRecordId = boost::none;

    // The number of records that were deleted and added before the cursor during the
    // collection scan.
    int64_t _numRecordChanges = 0;
    int64_t _dataSizeChanges = 0;

    // KeyString of the last processed index entry during the index scan.
    std::unique_ptr<KeyString> _lastProcessedIndexEntry = nullptr;

    // The current index namespace being scanned in the index scan phase.
    int _currentIndex = -1;

    // The stage that the validation is currently on.
    ValidationStage _stage = ValidationStage::DOCUMENT;

    // Contains the RecordId of when we should yield collection scan.
    boost::optional<RecordId> _yieldAtRecordId = boost::none;

    // Contains the KeyString of when we should yield during the index scan.
    std::unique_ptr<KeyString> _yieldAtIndexEntry = nullptr;

    // Threshold for the number of errors to record before returning "There are too many errors".
    static const int _kErrorThreshold = 100;

    // The current number of errors that are recorded.
    int _numErrorsRecorded = 0;

    // Only one thread can use the class at a time
    mutable stdx::mutex _classMutex;

    // If true, stops validation when `_canProceed()` is called.
    bool _shouldStopValidation = false;

    /**
     * Given the document's key KeyString, increment the corresponding `_indexKeyCount`
     * by hashing it.
     */
    void _addDocKey_inlock(const KeyString& ks, int indexNumber);

    /**
     * Given the document's key KeyString, decrement the corresponding `_indexKeyCount`
     * by hashing it.
     */
    void _removeDocKey_inlock(const KeyString& ks, int indexNumber);

    /**
     * Given the index entry's KeyString, decrement the corresponding `_indexKeyCount`
     * by hashing it.
     */
    void _addIndexKey_inlock(const KeyString& ks, int indexNumber);

    /**
     * Given the index entry's KeyString, increment the corresponding `_indexKeyCount`
     * by hashing it.
     */
    void _removeIndexKey_inlock(const KeyString& ks, int indexNumber);

    /**
     * Returns true if the index for the given `indexNs` has finished being scanned by
     * the validation, otherwise it returns false.
     */
    bool _isIndexFinished_inlock(int indexNumber) const;

    /**
     * Returns true if this is the current `indexNs` being scanned
     * by validation, otherwise it returns false.
     */
    bool _isIndexScanning_inlock(int indexNumber) const;

    /**
     * Allows the IndexObserver to set a yield point at `recordId` so that during the collection
     * scan we must yield before processing the record. This is a preventive measure so the
     * collection scan doesn't scan stale records.
     */
    void _setYieldAtRecordId_inlock(const RecordId recordId);

    /**
     * Allows the IndexObserver to set a yield point at the KeyString of `indexEntry` so that
     * during the index scan we must yield before processing the index entry.
     * This is a preventive measure so the index scan doesn't scan stale index entries.
     */
    void _setYieldAtIndexEntry_inlock(const KeyString& keyString);

    /**
     * Returns true if the `recordId` is before or equal to the last processed
     * RecordId.
     */
    bool _isBeforeLastProcessedRecordId_inlock(RecordId recordId) const;

    /**
     * Returns true if the `keyString` is before or equal to the last processed
     * index entry.
     */
    bool _isBeforeLastProcessedIndexEntry_inlock(const KeyString& keyString) const;

    /**
     * Returns a hashed value from the given KeyString and index namespace.
     */
    uint32_t _hashKeyString(const KeyString& ks, int indexNumbers) const;

    /**
     * Used alongside `yield()` and `relockCollectionWithMode()` to ensure that after the execution
     * of them it is safe to continue validating.
     * Validation can be stopped for a number of reasons including:
     * 1) The database was dropped.
     * 2) The collection was dropped.
     * 3) An index was added or removed in the collection being validated.
     * 4) The operation was killed.
     */
    Status _canProceed();

};  // IndexConsistency
}  // namespace mongo
