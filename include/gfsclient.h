#ifndef GFSCLIENT_H
#define GFSCLIENT_H

#define RETRY_SEND 3

#include <mutex>

#include "gfs.h"

typedef struct chunk_t {
  chunkid_t id;
  std::string checksum;
} chunk_t;

typedef struct file_handle_t {
  file_handle_t(std::string n, fmode_t m, uint64_t p, uint64_t i)
      : file_name(n), mode(m), ptr(p), inode(i){};
  std::string file_name;
  fmode_t mode;
  uint64_t ptr, inode;
} file_handle_t;

class GFSClient {
 public:
  GFSClient(const std::string& ip)
      : ip(ip),
        master(gfs_master_connect()),
        buff_map(std::map<uint64_t, char*>()),
        open_files(std::map<std::string, std::vector<uint64_t>>()){};

  file_handle_t* open(const std::string&, fmode_t);
  void close(file_handle_t*);
  int write(file_handle_t*, const void*, size_t);
  int read(file_handle_t*, void*, size_t);

 private:
  std::string ip;
  std::unique_ptr<gfs::Master::Stub> master;

  uint64_t _fh = 0;
  std::mutex _fh_mutex;

  std::mutex buffmap_mutex;
  std::map<uint64_t, char*> buff_map;

  std::mutex openfiles_mutex;
  std::map<std::string, std::vector<uint64_t>> open_files;

  chunk_handle_t* request_chunk(const std::string&, uint64_t);
  int send_chunk(const std::string&, chunkid_t, const void*);
  int flush(const chunk_handle_t*, const void*);
  int get_chunk(const chunk_handle_t*, void*);

  inline uint64_t _new_fh() {
    std::lock_guard<std::mutex> g(this->_fh_mutex);
    return this->_fh++;
  }
};

#endif /* GFSCLIENT_H */
