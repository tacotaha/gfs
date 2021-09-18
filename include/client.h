#ifndef CLIENT_H
#define CLIENT_H 

#include "gfs.h"

class Client {
    public:
        file_handle_t* Open(const std::string&);
};

#endif /* CLIENT_H */
