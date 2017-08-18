/*-
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/catalog/private/record_store_validate_adaptor.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/catalog/index_consistency.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/matcher/expression.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/storage/key_string.h"
#include "mongo/db/storage/record_store.h"
#include "mongo/rpc/object_check.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

namespace {

void processIndexEntry(IndexConsistency* indexConsistency,
                       const KeyString* indexKeyString,
                       const KeyString* prevIndexKeyString,
                       bool isFirstEntry,
                       int64_t* numKeys,
                       int64_t indexNumber,
                       ValidateResults* results) {

    // Ensure that the index entries are in increasing or decreasing order.
    if (!isFirstEntry && *indexKeyString < *prevIndexKeyString) {
        if (results->valid) {
            results->errors.push_back(
                "one or more indexes are not in strictly ascending or descending "
                "order");
        }
        results->valid = false;
    }

    indexConsistency->addIndexKey(*indexKeyString, indexNumber);
    (*numKeys)++;
}

void processRecord(RecordStoreValidateAdaptor* adaptor,
                   const boost::optional<Record>& curRecord,
                   const RecordId& prevRecordId,
                   int64_t* nrecords,
                   int64_t* dataSizeTotal,
                   int64_t* nInvalid,
                   ValidateResults* results) {

    ++(*nrecords);

    auto dataSize = curRecord->data.size();
    (*dataSizeTotal) += dataSize;
    size_t validatedSize;
    Status status = adaptor->validate(curRecord->id, curRecord->data, &validatedSize);

    // Checks to ensure isInRecordIdOrder() is being used properly.
    if (prevRecordId.isNormal()) {
        invariant(prevRecordId < curRecord->id);
    }

    // While some storage engines, such as MMAPv1, may use padding, we still require
    // that they return the unpadded record data.
    if (!status.isOK() || validatedSize != static_cast<size_t>(dataSize)) {
        if (results->valid) {
            // Only log once.
            results->errors.push_back("detected one or more invalid documents (see logs)");
        }
        (*nInvalid)++;
        results->valid = false;
        log() << "document at location: " << curRecord->id << " is corrupted";
    }
}
}  // namespace

Status RecordStoreValidateAdaptor::validate(const RecordId& recordId,
                                            const RecordData& record,
                                            size_t* dataSize) {
    BSONObj recordBson = record.toBson();

    const Status status = validateBSON(
        recordBson.objdata(), recordBson.objsize(), Validator<BSONObj>::enabledBSONVersion());
    if (status.isOK()) {
        *dataSize = recordBson.objsize();
    } else {
        return status;
    }

    if (!_indexCatalog->haveAnyIndexes()) {
        return status;
    }

    IndexCatalog::IndexIterator i = _indexCatalog->getIndexIterator(_opCtx, false);

    while (i.more()) {
        const IndexDescriptor* descriptor = i.next();
        const std::string indexNs = descriptor->indexNamespace();
        int indexNumber = _indexConsistency->getIndexNumber(indexNs);
        ValidateResults curRecordResults;

        const IndexAccessMethod* iam = _indexCatalog->getIndex(descriptor);

        if (descriptor->isPartial()) {
            const IndexCatalogEntry* ice = _indexCatalog->getEntry(descriptor);
            if (!ice->getFilterExpression()->matchesBSON(recordBson)) {
                (*_indexNsResultsMap)[indexNs] = curRecordResults;
                continue;
            }
        }

        BSONObjSet documentKeySet = SimpleBSONObjComparator::kInstance.makeBSONObjSet();
        // There's no need to compute the prefixes of the indexed fields that cause the
        // index to be multikey when validating the index keys.
        MultikeyPaths* multikeyPaths = nullptr;
        iam->getKeys(recordBson,
                     IndexAccessMethod::GetKeysMode::kEnforceConstraints,
                     &documentKeySet,
                     multikeyPaths);

        if (!descriptor->isMultikey(_opCtx) && documentKeySet.size() > 1) {
            std::string msg = str::stream() << "Index " << descriptor->indexName()
                                            << " is not multi-key but has more than one"
                                            << " key in document " << recordId;
            curRecordResults.errors.push_back(msg);
            curRecordResults.valid = false;
        }

        const auto& pattern = descriptor->keyPattern();
        const Ordering ord = Ordering::make(pattern);

        for (const auto& key : documentKeySet) {
            if (key.objsize() >= static_cast<int64_t>(KeyString::TypeBits::kMaxKeyBytes)) {
                // Index keys >= 1024 bytes are not indexed.
                _indexConsistency->addLongIndexKey(indexNumber);
                continue;
            }

            // We want to use the latest version of KeyString here.
            KeyString ks(KeyString::kLatestVersion, key, ord, recordId);
            _indexConsistency->addDocKey(ks, indexNumber);
        }
        (*_indexNsResultsMap)[indexNs] = curRecordResults;
    }
    return status;
}

void RecordStoreValidateAdaptor::traverseIndex(const IndexAccessMethod* iam,
                                               const IndexDescriptor* descriptor,
                                               bool background,
                                               ValidateResults* results,
                                               int64_t* numTraversedKeys) {

    invariant(_opCtx->lockState()->isCollectionLockedForMode(_nss.toString(), LockMode::MODE_X));

    auto indexNs = descriptor->indexNamespace();
    int indexNumber = _indexConsistency->getIndexNumber(indexNs);
    int64_t numKeys = 0;

    if (background) {
        // Notify the IndexConsistency instance that we're starting to scan a new index.
        _indexConsistency->notifyStartIndex(indexNumber);

        // Yield to get the latest snapshot before we begin scanning through the index.
        _indexConsistency->yield();
    }

    const auto& key = descriptor->keyPattern();
    const Ordering ord = Ordering::make(key);
    KeyString::Version version = KeyString::kLatestVersion;
    std::unique_ptr<KeyString> prevIndexKeyString = nullptr;
    bool isFirstEntry = true;

    int interruptInterval = 4096;
    boost::optional<IndexKeyEntry> currentIndexEntry = boost::none;

    std::unique_ptr<SortedDataInterface::Cursor> cursor = iam->newCursor(_opCtx, true);
    std::unique_ptr<SortedDataInterface::Cursor> lookAheadCursor = iam->newCursor(_opCtx, true);

    if (background) {
        // Allow concurrent read/writes to happen.
        _indexConsistency->relockCollectionWithMode(LockMode::MODE_IX);
    }

    // `shouldYield` will be true if we hit the periodic yield point to allow other operations
    // requiring locks to run.
    bool shouldYield = false;

    bool isEOF = false;
    while (!isEOF) {

        std::unique_ptr<KeyString> indexKeyString = nullptr;

        // `consumeAndReposition` will move the `cursor` forward once we get a new snapshot and
        // move the `lookAheadCursor` back to where `cursor` is. After that, we will process the
        // index entry.
        bool consumeAndReposition = false;
        if (!shouldYield) {

            boost::optional<IndexKeyEntry> lookAheadIndexEntry = boost::none;
            if (isFirstEntry) {
                // Seeking to BSONObj() is equivalent to seeking to the first entry of an index.
                lookAheadIndexEntry = lookAheadCursor->seek(BSONObj(), true);
            } else {
                lookAheadIndexEntry = lookAheadCursor->next();
            }

            if (!lookAheadIndexEntry) {
                isEOF = true;

                if (!background) {
                    // Non-background validation exits here.
                    continue;
                }
            } else {

                // We want to use the latest version of KeyString here.
                indexKeyString.reset(new KeyString(
                    version, lookAheadIndexEntry->key, ord, lookAheadIndexEntry->loc));

                // Checks to see if we should get a new snapshot or we can continue using this
                // snapshot because of our cursor placement.
                bool shouldGetNext = _indexConsistency->shouldGetNext(*indexKeyString);
                if (!shouldGetNext) {
                    shouldYield = true;
                    consumeAndReposition = true;
                } else {
                    // The look ahead cursor saw something, so we can safely move up our cursor up.
                    if (isFirstEntry) {
                        // Seeking to BSONObj() is equivalent to seeking to the first entry of an
                        // index.
                        currentIndexEntry = cursor->seek(BSONObj(), true);
                    } else {
                        currentIndexEntry = cursor->next();
                    }
                }
            }
        }

        if (background && (isEOF || shouldYield)) {

            // Switch to MODE_X to prohibit concurrency.
            _indexConsistency->relockCollectionWithMode(LockMode::MODE_X);

            // Save the cursor as we'll be abandoning the snapshot.
            cursor->save();
            lookAheadCursor->save();

            // We yield to get a new snapshot to be able to gather any records that may
            // have been added while we were waiting for get the MODE_X lock.
            _indexConsistency->yield();

            // Restore the cursor in the new snapshot.
            // If the `prevRecordId` that the cursor is pointing to gets removed in the
            // new snapshot, the call to next() will return the next closest position.
            cursor->restore();
            lookAheadCursor->restore();

            if (consumeAndReposition) {

                currentIndexEntry = cursor->next();
                if (!currentIndexEntry) {
                    isEOF = true;
                } else {
                    indexKeyString.reset(new KeyString(
                        version, currentIndexEntry->key, ord, currentIndexEntry->loc));

                    // This will always return true as we just yielded and are in MODE_X.
                    _indexConsistency->shouldGetNext(*indexKeyString);

                    boost::optional<IndexKeyEntry> lookAheadIndexEntry =
                        lookAheadCursor->seekExact(currentIndexEntry->key);
                    while (currentIndexEntry->loc != lookAheadIndexEntry->loc) {
                        lookAheadIndexEntry = lookAheadCursor->next();
                    }
                }
            }

            if (isEOF) {
                continue;
            }

            // Switch back to a MODE_IX lock to allow concurrency.
            _indexConsistency->relockCollectionWithMode(LockMode::MODE_IX);

            if (shouldYield && !consumeAndReposition) {
                shouldYield = false;
                continue;
            }
        }

        // Process the current index entry and prepare for the next iteration.
        processIndexEntry(_indexConsistency,
                          indexKeyString.get(),
                          prevIndexKeyString.get(),
                          isFirstEntry,
                          &numKeys,
                          indexNumber,
                          results);

        isFirstEntry = false;
        prevIndexKeyString.swap(indexKeyString);

        if (!(numKeys % interruptInterval)) {
            _opCtx->checkForInterrupt();
        }

        // Called last to ensure we don't end up in the loop forever.
        shouldYield = _indexConsistency->scanLimitHit();
    }

    invariant(_opCtx->lockState()->isCollectionLockedForMode(_nss.toString(), LockMode::MODE_X));

    while (((isFirstEntry) ? currentIndexEntry = cursor->seek(BSONObj(), true)
                           : currentIndexEntry = cursor->next())) {

        std::unique_ptr<KeyString> indexKeyString = nullptr;
        indexKeyString.reset(
            new KeyString(version, currentIndexEntry->key, ord, currentIndexEntry->loc));

        // Process the current index entry and prepare for the next iteration.
        processIndexEntry(_indexConsistency,
                          indexKeyString.get(),
                          prevIndexKeyString.get(),
                          isFirstEntry,
                          &numKeys,
                          indexNumber,
                          results);

        isFirstEntry = false;
        prevIndexKeyString.swap(indexKeyString);
    }

    *numTraversedKeys = _indexConsistency->getNumKeys(indexNumber);

    if (background) {
        _indexConsistency->notifyDoneIndex(indexNumber);
    }
}

void RecordStoreValidateAdaptor::traverseRecordStore(RecordStore* recordStore,
                                                     ValidateCmdLevel level,
                                                     ValidateResults* results,
                                                     BSONObjBuilder* output) {

    invariant(_opCtx->lockState()->isCollectionLockedForMode(_nss.toString(), LockMode::MODE_X));

    int64_t nrecords = 0;
    int64_t dataSizeTotal = 0;
    int64_t nInvalid = 0;

    results->valid = true;
    std::unique_ptr<SeekableRecordCursor> cursor = recordStore->getCursor(_opCtx, true);
    std::unique_ptr<SeekableRecordCursor> lookAheadCursor = recordStore->getCursor(_opCtx, true);
    int interruptInterval = 4096;

    boost::optional<Record> curRecord = boost::none;
    RecordId prevRecordId;

    // Allow concurrent read/writes to happen.
    _indexConsistency->relockCollectionWithMode(LockMode::MODE_IX);

    // `shouldYield` will be true if we hit the periodic yield point to allow other operations
    // requiring locks to run.
    bool shouldYield = false;

    bool isEOF = false;
    while (!isEOF) {

        // `consumeAndReposition` will move the `cursor` forward once we get a new snapshot and
        // move the `lookAheadCursor` back to where `cursor` is. After that, we will process the
        // record.
        bool consumeAndReposition = false;
        if (!shouldYield) {
            boost::optional<Record> lookAheadRecord = lookAheadCursor->next();
            if (!lookAheadRecord) {
                isEOF = true;
            } else {

                // Checks to see if we should get a new snapshot or we can continue using this
                // snapshot because of our cursor placement.
                bool shouldGetNext = _indexConsistency->shouldGetNext(lookAheadRecord->id);
                if (!shouldGetNext) {
                    shouldYield = true;
                    consumeAndReposition = true;
                } else {
                    // The look ahead cursor saw something, so we can safely move up our cursor up.
                    curRecord = cursor->next();
                }
            }
        }

        if (isEOF || shouldYield) {

            // Switch to MODE_X to prohibit concurrency.
            _indexConsistency->relockCollectionWithMode(LockMode::MODE_X);

            // Save the cursor as we'll be abandoning the snapshot.
            cursor->save();
            lookAheadCursor->save();

            // We yield to get a new snapshot to be able to gather any records that may
            // have been added while we were waiting for get the MODE_X lock.
            _indexConsistency->yield();

            // Restore the cursor in the new snapshot.
            // If the `prevRecordId` that the cursor is pointing to gets removed in the
            // new snapshot, the call to next() will return the next closest position.
            // If the cursor returns false, then the colletion object has been changed.
            bool cursorRestore = cursor->restore();
            uassert(40614, "Background validation was interrupted", cursorRestore);
            bool lookAheadCursorRestore = lookAheadCursor->restore();
            uassert(40615, "Background validation was interrupted", lookAheadCursorRestore);

            if (consumeAndReposition) {
                curRecord = cursor->next();
                if (!curRecord) {
                    isEOF = true;
                } else {
                    // This will always return true as we just yielded and are in MODE_X.
                    _indexConsistency->shouldGetNext(curRecord->id);

                    lookAheadCursor->seekExact(curRecord->id);
                }
            }

            if (isEOF) {
                continue;
            }

            // Switch back to a MODE_IX lock to allow concurrency.
            _indexConsistency->relockCollectionWithMode(LockMode::MODE_IX);

            if (shouldYield && !consumeAndReposition) {
                shouldYield = false;
                continue;
            }
        }

        // Process the current record and prepare for the next iteration.
        processRecord(this, curRecord, prevRecordId, &nrecords, &dataSizeTotal, &nInvalid, results);
        prevRecordId = curRecord->id;

        if (!(nrecords % interruptInterval)) {
            _opCtx->checkForInterrupt();
        }

        // Called last to ensure we don't end up in the loop forever.
        shouldYield = _indexConsistency->scanLimitHit();
    }

    invariant(_opCtx->lockState()->isCollectionLockedForMode(_nss.toString(), LockMode::MODE_X));

    while ((curRecord = cursor->next())) {

        // Process the current record and prepare for the next iteration.
        processRecord(this, curRecord, prevRecordId, &nrecords, &dataSizeTotal, &nInvalid, results);
        prevRecordId = curRecord->id;
    }

    nrecords += _indexConsistency->getNumRecordChangesBeforeCursor(&dataSizeTotal);

    _indexConsistency->nextStage();

    if (results->valid) {
        recordStore->updateStatsAfterRepair(_opCtx, nrecords, dataSizeTotal);
    }

    output->append("nInvalidDocuments", static_cast<long long>(nInvalid));
    output->appendNumber("nrecords", static_cast<long long>(nrecords));
}

void RecordStoreValidateAdaptor::validateIndexKeyCount(IndexDescriptor* idx,
                                                       int64_t numRecs,
                                                       ValidateResults& results) {
    const std::string indexNs = idx->indexNamespace();
    int indexNumber = _indexConsistency->getIndexNumber(indexNs);
    int64_t numIndexedKeys = _indexConsistency->getNumKeys(indexNumber);
    int64_t numLongKeys = _indexConsistency->getNumLongKeys(indexNumber);
    auto totalKeys = numLongKeys + numIndexedKeys;

    bool hasTooFewKeys = false;
    bool noErrorOnTooFewKeys = !failIndexKeyTooLong.load() && (_level != kValidateFull);

    if (idx->isIdIndex() && totalKeys != numRecs) {
        hasTooFewKeys = totalKeys < numRecs ? true : hasTooFewKeys;
        std::string msg = str::stream() << "number of _id index entries (" << numIndexedKeys
                                        << ") does not match the number of documents in the index ("
                                        << numRecs - numLongKeys << ")";
        if (noErrorOnTooFewKeys && (numIndexedKeys < numRecs)) {
            results.warnings.push_back(msg);
        } else {
            results.errors.push_back(msg);
            results.valid = false;
        }
    }

    if (results.valid && !idx->isMultikey(_opCtx) && totalKeys > numRecs) {
        std::string err = str::stream()
            << "index " << idx->indexName() << " is not multi-key, but has more entries ("
            << numIndexedKeys << ") than documents in the index (" << numRecs - numLongKeys << ")";
        results.errors.push_back(err);
        results.valid = false;
    }
    // Ignore any indexes with a special access method. If an access method name is given, the
    // index may be a full text, geo or special index plugin with different semantics.
    if (results.valid && !idx->isSparse() && !idx->isPartial() && !idx->isIdIndex() &&
        idx->getAccessMethodName() == "" && totalKeys < numRecs) {
        hasTooFewKeys = true;
        std::string msg = str::stream()
            << "index " << idx->indexName() << " is not sparse or partial, but has fewer entries ("
            << numIndexedKeys << ") than documents in the index (" << numRecs - numLongKeys << ")";
        if (noErrorOnTooFewKeys) {
            results.warnings.push_back(msg);
        } else {
            results.errors.push_back(msg);
            results.valid = false;
        }
    }

    if ((_level != kValidateFull) && hasTooFewKeys) {
        std::string warning = str::stream()
            << "index " << idx->indexName()
            << " has fewer keys than records. This may be the result of currently or "
               "previously running the server with the failIndexKeyTooLong parameter set to "
               "false. Please re-run the validate command with {full: true}";
        results.warnings.push_back(warning);
    }
}
}  // namespace
