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
  grpc::Status NewChunk(grpc::ServerContext*, const gfs::ChunkID*,
                        gfs::Status*) override;
  grpc::Status GetChunk(grpc::ServerContext*, const gfs::ChunkID*,
                        gfs::Chunk*) override;
  grpc::Status RemoveChunk(grpc::ServerContext*, const gfs::ChunkID*,
                           gfs::Status*) override;
  grpc::Status SetLease(grpc::ServerContext*, const gfs::Lease*,
                        gfs::Status*) override;
  grpc::Status RevokeLease(grpc::ServerContext*, const gfs::ChunkID*,
                           gfs::Status*) override;

 private:
  void master_connect();
  void heartbeat();
  void chunk_report();
  int write_chunk(const gfs::Chunk*);
  int get_chunk(chunkid_t, void*);
  int extend_lease(chunkid_t cid);
  int send_heartbeat(gfs::Status*);
  int send_chunk_report(gfs::Status*);
  gfs::Chunk new_chunk(chunkid_t);

  std::string IP;
  std::string server_id;
  std::filesystem::path chunk_dir;
  std::vector<std::thread> tids;

  // chunkid -> checksum
  std::map<chunkid_t, std::string> chunks;
  std::mutex chunks_mutex;

  // chunks for which this server is the primary
  std::map<chunkid_t, time_t> leases;
  std::mutex leases_mutex;

  std::unique_ptr<gfs::Master::Stub> master;

  inline std::string _get_path(chunkid_t cid) {
    return std::filesystem::path(this->chunk_dir) / std::to_string(cid);
  }
};

#endif /* CSERVER_H */
