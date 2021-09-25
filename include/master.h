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
typedef std::pair<std::string, uint64_t> server_ts_t;

class GFSMaster final : public gfs::Master::Service {
 public:
  GFSMaster(const std::string&);
  ~GFSMaster();

  // rpc service calls
  grpc::Status HeartBeat(grpc::ServerContext*, const gfs::HBPayload*,
                         gfs::Status*) override;
  grpc::Status ChunkReport(grpc::ServerContext*, const gfs::CRPayload*,
                           gfs::Status*) override;
  grpc::Status Open(grpc::ServerContext*, const gfs::OpenPayload*,
                    gfs::FileHandle*) override;
  grpc::Status Remove(grpc::ServerContext*, const gfs::FileHandle*,
                      gfs::Status*) override;
  grpc::Status RequestChunk(grpc::ServerContext*, const gfs::FileHandle*,
                            gfs::RCResp*) override;
  grpc::Status ExtendLease(grpc::ServerContext*, const gfs::ELPayload*,
                           gfs::Lease*) override;

 private:
  std::string IP;
  std::vector<std::thread> tids;

  // filename -> chunkset
  std::map<std::string, chunkset_t> chunks;
  std::mutex chunks_mutex;

  // chunk server IP -> ts last seen
  std::map<std::string, uint64_t> chunk_servers;
  std::mutex chunkservers_mutex;

  // chunk -> servers
  std::map<chunkid_t, std::set<std::string>> chunk_locs;
  std::mutex chunklocs_mutex;

  // primary lease info
  std::map<chunkid_t, server_ts_t> lease_info;
  std::mutex leaseinfo_mutex;

  // global chunk id counter
  chunkid_t chunk_ctr;
  std::mutex chunk_ctr_mutex;

  void StartRPCServer();
  void get_servers(std::vector<std::string>&);
  void update_leases();
  int assign_chunk(const std::string&, chunkid_t);
  int remove_chunk(const std::string&, chunkid_t);
  int send_chunk(const std::string&);
  int set_lease(chunkid_t, const server_ts_t&);
  int revoke_lease(const std::string&, chunkid_t);
  std::string rand_server(chunkid_t);
  chunkid_t new_chunkid();
  chunkid_t new_chunk(const std::string&);

  inline uint64_t _get_ts() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
  }
};

#endif /* MASTER_H */
