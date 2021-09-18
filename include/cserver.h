#ifndef CSERVER_H
#define CSERVER_H 

#include <iostream>
#include <memory>
#include <thread>
#include <map>

#include "gfs.grpc.pb.h"
#include "gfs.h"

class CServer{
    public:
        CServer(const std::string&);
        ~CServer();
    private:
        void master_connect();
        int send_heartbeat(gfs::HBResp*);
        void heartbeat();

        std::string IP;
        std::thread hb_tid;
        std::map<chunkid_t, std::string> chunks;
        std::unique_ptr<gfs::Master::Stub> master;
};

#endif /* CSERVER_H */
