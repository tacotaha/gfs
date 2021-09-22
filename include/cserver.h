#ifndef CSERVER_H
#define CSERVER_H

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <filesystem>
#include <iostream>
#include <map>
#include <memory>
#include <thread>

#include "gfs.grpc.pb.h"
#include "gfs.h"

class CServer final : public gfs::CServer::Service {
 public:
  CServer(const std::string&);
  ~CServer();

  // rpc service calls
  grpc::Status SendChunk(grpc::ServerContext*, const gfs::Chunk*,
                         gfs::Status*) override;
  grpc::Status NewChunk(grpc::ServerContext*, const gfs::NCPayload*,
                        gfs::Status*) override;
  grpc::Status GetChunk(grpc::ServerContext*, const gfs::ChunkID*, gfs::Chunk*);

 private:
  void master_connect();
  int send_heartbeat(gfs::Status*);
  void heartbeat();

  gfs::Chunk new_chunk(chunkid_t);
  int write_chunk(const gfs::Chunk*);
  int get_chunk(chunkid_t, void*);

  std::string IP;
  std::string server_id;
  std::filesystem::path chunk_dir;
  std::thread hb_tid;

  // chunkid -> <checksum, primary>
  std::map<chunkid_t, chunkserver_t> chunks;
  std::mutex chunks_mutex;

  std::unique_ptr<gfs::Master::Stub> master;

  inline std::string _get_path(chunkid_t cid) {
    return std::filesystem::path(this->chunk_dir) / std::to_string(cid);
  }
};

#endif /* CSERVER_H */
