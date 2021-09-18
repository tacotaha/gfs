#ifndef MASTER_H
#define MASTER_H 

#include <string>
#include <memory>
#include <thread>
#include <ctime>
#include <map>
#include <mutex>
#include <sys/stat.h>
#include <sys/types.h>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "gfs.grpc.pb.h"
#include "gfs.h"

typedef std::vector<chunkid_t> chunkset_t;
typedef std::pair<std::string, bool> chunkserver_t;

class GFSMaster final : public gfs::Master::Service{
    public:
        GFSMaster(const std::string&);        

        // rpc service calls
        grpc::Status HeartBeat(grpc::ServerContext*, const gfs::HBPayload*, 
                               gfs::HBResp*) override;
        grpc::Status Open(grpc::ServerContext*, const gfs::OpenPayload*, 
                          gfs::OpenResp*) override;
    private:
        void StartRPCServer(); 
        chunkid_t new_chunkid();
        chunkid_t new_chunk(const std::string&);

        std::string IP;

        // filename -> chunkset
        std::map<std::string, chunkset_t> chunks;
        std::mutex chunks_mutex;
        
        // chunk server IP -> timestamp of last contact
        std::map<std::string, std::time_t> chunk_servers;
        std::mutex chunkservers_mutex;

        // chunkids -> server locations
        std::map<chunkid_t, std::set<chunkserver_t>>chunk_locs;
        std::mutex chunklocs_mutex;

        chunkid_t chunk_ctr;
        std::mutex chunk_ctr_mutex;
};

#endif /* MASTER_H */
