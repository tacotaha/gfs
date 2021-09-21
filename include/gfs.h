#ifndef GFS_H
#define GFS_H

#define BASE_IP "127.0.0.1"
#define BASE_PORT "8000"
#define CHUNK_DIR ".chunks"

#define HB_INTERVAL_MS (2 * 1000)
#define CHUNK_SIZE (1 << 6)
#define RFACTOR 3

#define READ (1 << 0)
#define WRITE (1 << 1)
#define APPEND (1 << 2)

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <memory>
#include <string>

#include "gfs.grpc.pb.h"

typedef uint64_t chunkid_t;
typedef uint8_t fmode_t;
typedef unsigned char uchar_t;
typedef std::pair<std::string, bool> chunkserver_t;

typedef struct chunk_handle_t {
  chunkid_t cid;
  chunkserver_t loc[RFACTOR];
} chunk_handle_t;

std::string sha256sum(const void* payload, size_t len);
std::unique_ptr<gfs::Master::Stub> gfs_master_connect();
void dump_buff(const char*);

#endif /* GFS_H */
