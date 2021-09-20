#ifndef GFSCLIENT_H
#define GFSCLIENT_H 

#include "gfs.h"

typedef struct chunk_t {
    chunkid_t id;
    std::string checksum;
} chunk_t;

typedef struct file_handle_t {
    file_handle_t(std::string n, fmode_t m, uint64_t p) : file_name(n), mode(m), ptr(p) {};
    std::string file_name;
    fmode_t mode;
    uint64_t ptr;
} file_handle_t;

class GFSClient {
    public:
        GFSClient(const std::string &ip) : ip(ip), master(gfs_master_connect()) {}; 
        file_handle_t* open(const std::string&, fmode_t);
        int write(file_handle_t*, const void*, size_t);
        int read(file_handle_t*, void*, size_t);
    private:
        std::string ip;
        std::unique_ptr<gfs::Master::Stub> master;
        chunk_handle_t* request_chunk(const std::string&, uint64_t);
};

#endif /* GFSCLIENT_H */
