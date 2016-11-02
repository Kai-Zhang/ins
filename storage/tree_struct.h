#ifndef _GALAXY_INS_TREE_STRUCTURE_H_
#define _GALAXY_INS_TREE_STRUCTURE_H_
#include "storage/structure.h"
#include <boost/lexical_cast.hpp>
#include <vector>
#include <algorithm>

namespace galaxy {
namespace ins {

class StorageManager; // forward declaration

/**
 * @brief Structure provides tree-style data i/o
 */
class TreeStructure : public BasicStructure {
public:
    /// the structure needs a underlying data storage
    /// and will not owner nor release this pointer
    TreeStructure(StorageManager* store) : _underlying(store) { }
    virtual ~TreeStructure() { }

    virtual Status Get(std::string& info, const std::string& ns,
            const std::string& key) const;
    virtual Status Put(const std::string& ns, const std::string& key,
            const std::string& info);
    /// removes an empty node which has no children nodes
    /// returns INVALID if the node is not empty
    virtual Status Delete(const std::string& ns, const std::string& key);
    /**
     * @brief Returns a iterator over the children of the key
     * @param ns   [IN] namespace of the specified key
     * @param key  [IN] the parent directory to list
     * @return     a StructureIterator pointer
     *             if the namespace is not exist, or the directory is empty or inexist,
     *             the iterator will be done immediately
     */
    virtual StructureIterator* List(const std::string& ns,
            const std::string& key) const;
private:
    /// returns the key used in underlying storage
    std::string GetStructuredKey(const std::string& key) const {
        int level = std::count(key.begin(), key.end(), '/');
        // ignore the trailing /
        if (*key.rbegin() == '/') {
            --level;
        }
        return boost::lexical_cast<std::string>(level) + '#' + key;
    }

    /// returns the index used to scan underlying storage
    std::string GetListKey(const std::string& key) const {
        // add one more trailing / to increase level number
        std::string prefix = key;
        if (*prefix.rbegin() != '/') {
            prefix.push_back('/');
        }
        int level = std::count(prefix.begin(), prefix.end(), '/');
        return boost::lexical_cast<std::string>(level) + '#' + prefix;
    }

    /// strip the last level and get the parent directory string
    std::string GetParent(const std::string& key) const {
        // ignore the possible trailing /
        size_t last_sep = key.find_last_of('/', key.length() - 2);
        return last_sep != std::string::npos ? key.substr(0, last_sep) : "";
    }
private:
    StorageManager* _underlying;
};

} // namespace ins
} // namespace galaxy

#endif // _GALAXY_INS_TREE_STRUCTURE_H_

