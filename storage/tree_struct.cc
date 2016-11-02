#include "tree_struct.h"

#include <boost/scoped_ptr.hpp>
#include "storage/storage_manage.h"

namespace galaxy {
namespace ins {

/// iterator on the tree structure
class TreeIterator : public StructureIterator {
public:
    TreeIterator(StorageManager::Iterator* it, const std::string& prefix) :
            _it(it), _prefix(prefix) {
        if (Done()) {
            return;
        }
        _key = GetOriginKey(_it->key());
    }
    virtual ~TreeIterator() { }

    virtual std::string Key() const {
        return _key;
    }

    virtual std::string Value() const {
        return _it->value();
    }

    virtual bool Done() const {
        return !_it->Valid() || (_it->key().compare(0, _prefix.length(), _prefix) != 0);
    }

    virtual StructureIterator* Next() {
        _it->Next();
        if (!Done()) {
            _key = GetOriginKey(_it->key());
        }
        return this;
    }

    virtual Status Error() const {
        return _it->status();
    }

private:
    /// returns the original key of a structured key in underlying storage
    std::string GetOriginKey(const std::string& structured) const {
        size_t sep = structured.find_first_of('#');
        return sep != std::string::npos ? structured.substr(sep + 1) : structured;
    }

private:
    boost::scoped_ptr<StorageManager::Iterator> _it;
    // record parent directory and abort scanning accordingly
    std::string _prefix;
    std::string _key;
};

Status TreeStructure::Get(std::string& value, const std::string& ns,
        const std::string& key) const {
    return _underlying->Get(ns, GetStructuredKey(key), &value);
}

Status TreeStructure::Put(const std::string& ns, const std::string& key,
        const std::string& value) {
    // put current node to database
    Status ret = _underlying->Put(ns, GetStructuredKey(key), value);
    if (ret != kOk) {
        return ret;
    }
    // loop to create parent node
    std::string parent = key;
    while ((parent = GetParent(parent)) != "") {
        // get current node value
        std::string cur_value;
        ret = _underlying->Get(ns, GetStructuredKey(parent), &cur_value);
        if (ret != kOk && ret != kNotFound) {
            // database error
            return ret;
        }
        if (ret != kOk) {
            // node has not existed, create an empty node
            if (_underlying->Put(ns, GetStructuredKey(parent), "") != kOk) {
                return kError;
            }
        } else {
            break;
        }
    }
    return kOk;
}

Status TreeStructure::Delete(const std::string& ns, const std::string& key) {
    boost::scoped_ptr<StructureIterator> it(List(ns, key));
    if (!it->Done()) {
        return kError;
    }
    return _underlying->Delete(ns, GetStructuredKey(key));
}

StructureIterator* TreeStructure::List(const std::string& ns,
        const std::string& key) const {
    StorageManager::Iterator* it = _underlying->NewIterator(ns);
    if (it == NULL) {
        return NULL;
    }
    const std::string& list_key = GetListKey(key);
    return new TreeIterator(it->Seek(list_key), list_key);
}

} // namespace ins
} // namespace galaxy

