#ifndef __BASE_CLASSES__
#define __BASE_CLASSES__

#include <e/buffer.h>

namespace node_prog
{
    class Packable 
    {
        public:
            virtual uint64_t size() const  = 0;
            virtual void pack(e::buffer::packer& packer) const = 0;
            virtual void unpack(e::unpacker& unpacker) = 0;
    };

    class Deletable 
    {
        public:
            virtual ~Deletable() = 0;
    };

    inline Deletable::~Deletable() 
    { 
        /* destructor must be defined */ 
    }

//    class Packable_Deletable : public virtual Packable, public virtual Deletable { };

    class Node_Parameters_Base : public virtual Packable
    {
        virtual bool search_cache() = 0; // how is this packabale?
        virtual uint64_t cache_key() = 0;
    };

    class Node_State_Base : public virtual Packable, public virtual Deletable 
    {

    };

    class Cache_Value_Base : public virtual Packable, public virtual Deletable
    {

    };
}

#endif
