#ifndef MASTER_H
#define MASTER_H

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <ctime>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "gfs.grpc.pb.h"
#include "gfs.h"

typedef std::vector<chunkid_t> chunkset_t;
typedef std::pair<std::time_t, uint64_t> cs_stats_t;
typedef std::pair<std::string, cs_stats_t> cs_kv_t;

class GFSMaster final : public gfs::Master::Service {
 public:
  GFSMaster(const std::string&);

  // rpc service calls
  grpc::Status HeartBeat(grpc::ServerContext*, const gfs::HBPayload*,
                         gfs::Status*) override;
  grpc::Status Open(grpc::ServerContext*, const gfs::OpenPayload*,
                    gfs::FileHandle*) override;
  grpc::Status RequestChunk(grpc::ServerContext*, const gfs::FileHandle*,
                            gfs::RCResp*) override;

 private:
  std::string IP;

  // filename -> chunkset
  std::map<std::string, chunkset_t> chunks;
  std::mutex chunks_mutex;

  // chunk server IP -> <last seen, numchunks>
  std::map<std::string, cs_stats_t> chunk_servers;
  std::mutex chunkservers_mutex;

  // chunkids -> server locations
  std::map<chunkid_t, std::set<chunkserver_t>> chunk_locs;
  std::mutex chunklocs_mutex;

  chunkid_t chunk_ctr;
  std::mutex chunk_ctr_mutex;

  void StartRPCServer();
  chunkid_t new_chunkid();

  chunkid_t new_chunk(const std::string&);
  int assign_chunk(const std::string&, chunkid_t, bool);

  int send_chunk(const std::string&);
  void get_servers(std::vector<std::string>&);
};

#endif /* MASTER_H */
