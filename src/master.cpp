#include "master.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

GFSMaster::GFSMaster(const std::string &ip) : IP(ip) {
  this->chunks = std::map<std::string, chunkset_t>();
  this->chunk_servers = std::map<std::string, cs_stats_t>();
  this->chunk_locs = std::map<chunkid_t, std::set<chunkserver_t>>();
  this->chunk_ctr = 0;
}

int GFSMaster::send_chunk(const std::string &ip_addr) {
  auto c = grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));
  chunkid_t cid = this->new_chunkid();

  std::ifstream rf("/dev/random", std::ios::out | std::ios::binary);
  if (!rf) return 1;

  char rand_bytes[CHUNK_SIZE];
  rf.read(rand_bytes, CHUNK_SIZE);
  rf.close();

  std::string checksum = sha256sum(rand_bytes, CHUNK_SIZE);

  grpc::ClientContext ctx;
  gfs::Status r;
  gfs::Chunk chunk;

  chunk.set_data(rand_bytes, CHUNK_SIZE);
  chunk.mutable_chunk()->set_id(cid);
  chunk.mutable_chunk()->set_checksum(checksum);

  return client->SendChunk(&ctx, chunk, &r).ok();
}

int GFSMaster::assign_chunk(const std::string &ip_addr, chunkid_t cid,
                            bool primary) {
  auto c = grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  grpc::ClientContext ctx;
  gfs::Status r;
  gfs::NCPayload p;

  p.set_id(cid);
  p.set_primary(primary);

  return client->NewChunk(&ctx, p, &r).ok();
}

void GFSMaster::get_servers(std::vector<std::string> &servers) {
  servers.clear();
  std::lock_guard<std::mutex> g(this->chunkservers_mutex);
  std::vector<cs_kv_t> vec(this->chunk_servers.begin(),
                           this->chunk_servers.end());

  // sort servers by disk usage
  std::sort(vec.begin(), vec.end(),
            [](const cs_kv_t &a, const cs_kv_t &b) -> bool {
              return a.second.second < b.second.second;
            });

  for (const auto &x : vec) servers.push_back(x.first);
}

grpc::Status GFSMaster::HeartBeat(grpc::ServerContext *ct,
                                  const gfs::HBPayload *p, gfs::Status *r) {
  time_t ts = std::time(0);
  std::cout << ts << ": " << p->id() << " has " << p->numchunks() << " chunks"
            << std::endl;

  std::lock_guard<std::mutex> g(this->chunkservers_mutex);
  this->chunk_servers[p->id()] = std::make_pair(ts, p->numchunks());
  r->set_status(true);
  return grpc::Status::OK;
}

grpc::Status GFSMaster::Open(grpc::ServerContext *ct, const gfs::OpenPayload *p,
                             gfs::FileHandle *r) {
  fmode_t mode = p->mode();
  std::string file_name = p->filename();

  std::cout << "Open(" << p->id() << ", " << p->filename() << ", " << p->mode()
            << ")" << std::endl;

  std::lock_guard<std::mutex> g(this->chunks_mutex);
  if (this->chunks.find(file_name) == this->chunks.end() &&
      (mode & (READ | APPEND))) {
    return grpc::Status::CANCELLED;
  }

  r->set_filename(file_name);
  r->set_ptr(0);

  return grpc::Status::OK;
}

grpc::Status GFSMaster::RequestChunk(grpc::ServerContext *ctx,
                                     const gfs::FileHandle *fh,
                                     gfs::RCResp *r) {
  chunkid_t cid;
  bool new_file = false;
  gfs::ChunkServer *loc;
  std::string file_name = fh->filename();
  uint64_t chunk_index = (fh->ptr() / CHUNK_SIZE);

  std::cout << "RequestChunk(" << file_name << ", " << chunk_index << ")"
            << std::endl;

  this->chunks_mutex.lock();
  auto cit = this->chunks.find(file_name);
  new_file = (cit == this->chunks.end());
  this->chunks_mutex.unlock();

  if (!new_file && cit->second.size() < chunk_index)
    return grpc::Status::CANCELLED;
  else if (new_file || cit->second.size() == chunk_index)
    cid = this->new_chunk(file_name);
  else
    cid = cit->second.at(chunk_index);

  r->set_chunkid(cid);

  std::lock_guard<std::mutex> l(this->chunklocs_mutex);
  const auto lit = this->chunk_locs.find(cid);
  if (lit == this->chunk_locs.end()) return grpc::Status::CANCELLED;

  for (auto const &x : lit->second) {
    loc = r->add_loc();
    loc->set_id(x.first);
    loc->set_primary(x.second);
  }

  return grpc::Status::OK;
}

chunkid_t GFSMaster::new_chunkid() {
  std::lock_guard<std::mutex> g(this->chunk_ctr_mutex);
  return this->chunk_ctr++;
}

chunkid_t GFSMaster::new_chunk(const std::string &fname) {
  std::vector<std::string> servers;
  chunkid_t cid = this->new_chunkid();
  this->get_servers(servers);

  // assign chunk locations
  std::lock_guard<std::mutex> g(this->chunklocs_mutex);
  this->chunk_locs[cid] = std::set<chunkserver_t>();
  for (int i = 0; i < RFACTOR; ++i) {
    this->chunk_locs[cid].insert(std::make_pair(servers[i], !i));
    this->assign_chunk(servers[i], cid, !i);
  }

  // add chunk to file's chunkset
  std::lock_guard<std::mutex> l(this->chunks_mutex);
  const auto &it = this->chunks.find(fname);
  if (it == this->chunks.end()) this->chunks[fname] = chunkset_t();
  this->chunks[fname].push_back(cid);

  return cid;
}

int main() {
  auto master_ip = std::string(BASE_IP) + ":" + BASE_PORT;

  GFSMaster service(master_ip);
  grpc::ServerBuilder builder;

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  builder.AddListeningPort(master_ip, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);

  std::cout << "Master server listening on: " << master_ip << std::endl;
  std::unique_ptr<grpc::Server> s(builder.BuildAndStart());

  s->Wait();

  return 0;
}
