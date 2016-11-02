#ifndef _GALAXY_INS_KV_STRUCT_H_
#define _GALAXY_INS_KV_STRUCT_H_
#include "storage/structure.h"

#include <boost/scoped_ptr.hpp>
#include "storage/storage_manage.h"

namespace galaxy {
namespace ins {

/// iterator on the kv structure
class KVIterator : public StructureIterator {
public:
    KVIterator(StorageManager::Iterator* it) : _it(it) { }
    virtual ~KVIterator() { }

    virtual std::string Key() const {
        return _key;
    }

    virtual std::string Value() const {
        return _it->value();
    }

    virtual bool Done() const {
        return !_it->Valid();
    }

    virtual StructureIterator* Next() {
        _it->Next();
        if (_it->Valid()) {
            _key = GetOriginKey(_it->key());
        }
        return this;
    }

private:
    /// returns the original key of a structured key in underlying storage
    std::string GetOriginKey(const std::string& structured) const {
        return structured.substr(1);
    }

private:
    boost::scoped_ptr<StorageManager::Iterator> _it;
    std::string _key;
};

/**
 * @brief Structure provides plain key-value data i/o
 */
class KVStructure : public BasicStructure {
public:
    /// the structure needs a underlying data storage
    /// and will not owner nor release this pointer
    KVStructure(StorageManager* store) : _underlying(store) { }
    virtual ~KVStructure() { }

    virtual Status Get(std::string& value, const std::string& ns,
            const std::string& key) const {
        return _underlying->Get(ns, GetStructuredKey(key), &value);
    }

    virtual Status Put(const std::string& ns, const std::string& key,
            const std::string& value) {
        return _underlying->Put(ns, GetStructuredKey(key), value);
    }

    virtual Status Delete(const std::string& ns, const std::string& key) {
        return _underlying->Delete(ns, GetStructuredKey(key));
    }

    /**
     * @brief Returns a iterator starting from the given key
     * @param ns   [IN] namespace of the specified key
     * @param key  [IN] start key to list
     * @return     a StructureIterator pointer
     *             the iterator will automatically seek to the key
     */
    virtual StructureIterator* List(const std::string& ns,
            const std::string& key) const {
        StorageManager::Iterator* it = _underlying->NewIterator(ns);
        return new KVIterator(it->Seek(GetStructuredKey(key)));
    }
private:
    /// returns the key used in underlying storage
    std::string GetStructuredKey(const std::string& key) const {
        return std::string(".") + key;
    }
private:
    StorageManager* _underlying;
};

} // namespace ins
} // namespace galaxy

#endif // _GALAXY_INS_KV_STRUCT_H_

