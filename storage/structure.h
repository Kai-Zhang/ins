#ifndef _GALAXY_INS_STRUCTURE_H_
#define _GALAXY_INS_STRUCTURE_H_

#include <string>
#include "proto/ins_node.pb.h"

namespace galaxy {
namespace ins {

/// iterator over structured data
class StructureIterator {
public:
    virtual std::string Key() const = 0;
    virtual std::string Value() const = 0;
    virtual bool Done() const = 0;
    virtual StructureIterator* Next() = 0;

    virtual ~StructureIterator() { }
};

/// interface of structure storage
/// using underlying data storage to provide structured data i/o
class BasicStructure {
public:
    virtual Status Get(std::string& value, const std::string& ns,
            const std::string& key) const = 0;
    virtual Status Put(const std::string& ns, const std::string& key,
            const std::string& value) = 0;
    virtual Status Delete(const std::string& ns, const std::string& key) = 0;
    /**
     * @brief Returns a iterator using the given key,
     *        the function has different definition in different implements
     * @param ns   [IN] namespace of the data
     * @param key  [IN] key for the iterator to start
     * @return     a StructureIterator pointer over a list of data
     */
    virtual StructureIterator* List(const std::string& ns,
            const std::string& key) const = 0;

    virtual ~BasicStructure() { }
};

} // namespace ins
} // namespace galaxy

#endif // _GALAXY_INS_STRUCTURE_H_

