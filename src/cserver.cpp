#include <chrono>
#include <grpcpp/grpcpp.h>

#include "cserver.h"

CServer::CServer(const std::string& ip) : IP(ip), chunks(std::map<chunkid_t, std::string>()){
    this->master_connect();
    this->hb_tid = std::thread{&CServer::heartbeat, this};
}

CServer::~CServer() {
    this->hb_tid.join();
}

void CServer::master_connect(){
    auto master_ip = std::string(BASE_IP) + ":" + BASE_PORT;
    std::cout << "Connecting to: " << master_ip << std::endl;
    auto c = grpc::CreateChannel(master_ip, grpc::InsecureChannelCredentials());
    this->master = std::move(gfs::Master::NewStub(c));
}

int CServer::send_heartbeat(gfs::HBResp *r){
    grpc::ClientContext c;
    gfs::HBPayload p;

    p.set_numchunks(10);
    p.set_id(this->IP);
    auto status = this->master->HeartBeat(&c, p, r);

    if(!status.ok())
        std::cout << status.error_code() << ": " << status.error_message() << std::endl;

    return status.ok();
}

void CServer::heartbeat() {
    int res;
    gfs::HBResp r;

    while(1) {
        res = this->send_heartbeat(&r);
        if(!res){
            std::cerr << "Heartbeat error" << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(HB_INTERVAL_MS));
    }
}

int main(int argc, char* argv[]){ 
    if(argc < 2) {
        std::cerr << "Usage: " << argv[0] << " PORT" << std::endl;
        return 1; 
    }
    
    auto port = std::string(BASE_PORT) + argv[1];
    auto ip_addr = std::string(BASE_IP) + ":" + argv[1];

    CServer c(ip_addr);

    return 0;
}
