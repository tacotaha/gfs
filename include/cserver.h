#ifndef CSERVER_H
#define CSERVER_H 

#include <iostream>
#include <memory>
#include <thread>
#include <map>

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "gfs.grpc.pb.h"
#include "gfs.h"

class CServer final : public gfs::CServer::Service {
    public:
        CServer(const std::string&);
        ~CServer();

        // rpc service calls
        grpc::Status SendChunk(grpc::ServerContext*, const gfs::Chunk*, gfs::Status*) override;
        grpc::Status NewChunk(grpc::ServerContext*, const gfs::NCPayload*, gfs::Status*) override;
    private:
        void master_connect();
        int send_heartbeat(gfs::Status*);
        void heartbeat();
        gfs::Chunk new_chunk(chunkid_t);
        int write_chunk(const gfs::Chunk*);

        std::string IP;
        std::thread hb_tid;

        // chunkid -> <checksum, primary>
        std::map<chunkid_t, chunkserver_t> chunks;
        std::mutex chunks_mutex;

        std::unique_ptr<gfs::Master::Stub> master;
};

#endif /* CSERVER_H */
