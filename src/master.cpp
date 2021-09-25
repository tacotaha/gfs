#include "master.h"

#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

GFSMaster::GFSMaster(const std::string &ip)
    : IP(ip),
      tids(std::vector<std::thread>()),
      chunks(std::map<std::string, chunkset_t>()),
      chunk_servers(std::map<std::string, uint64_t>()),
      chunk_locs(std::map<chunkid_t, std::set<std::string>>()),
      lease_info(std::map<uint64_t, server_ts_t>()),
      chunk_ctr(0) {
  srand(std::time(0));
  this->tids.push_back(std::thread{&GFSMaster::update_leases, this});
}

GFSMaster::~GFSMaster() {
  for (size_t i = 0; i < this->tids.size(); ++i) this->tids[i].join();
}

int GFSMaster::assign_chunk(const std::string &ip_addr, chunkid_t cid) {
  auto c = grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  grpc::ClientContext ctx;
  gfs::Status r;
  gfs::ChunkID p;

  p.set_id(cid);

  auto status = client->NewChunk(&ctx, p, &r);

  if (status.ok()) {
    std::lock_guard<std::mutex> g(this->chunklocs_mutex);
    if (this->chunk_locs.find(cid) == this->chunk_locs.end())
      this->chunk_locs[cid] = std::set<std::string>();
    this->chunk_locs[cid].insert(ip_addr);
  }

  return status.ok();
}

int GFSMaster::remove_chunk(const std::string &ip_addr, chunkid_t cid) {
  auto c = grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  grpc::ClientContext ctx;
  gfs::ChunkID id;
  gfs::Status r;
  id.set_id(cid);

  return client->RemoveChunk(&ctx, id, &r).ok();
}

void GFSMaster::get_servers(std::vector<std::string> &servers) {
  servers.clear();
  std::vector<server_ts_t> res(RFACTOR);
  std::lock_guard<std::mutex> g(this->chunkservers_mutex);
  std::sample(this->chunk_servers.begin(), this->chunk_servers.end(),
              res.begin(), RFACTOR, std::mt19937{std::random_device{}()});
  for (int i = 0; i < RFACTOR; ++i) servers.push_back(res[i].first);
}

std::string GFSMaster::rand_server(chunkid_t cid) {
  std::string server;
  std::vector<std::string> res(1);
  std::lock_guard<std::mutex> g(this->chunklocs_mutex);

  auto it = this->chunk_locs.find(cid);
  if (it != this->chunk_locs.end()) {
    std::sample(it->second.begin(), it->second.end(), res.begin(), 1,
                std::mt19937{std::random_device{}()});
    server = *res.begin();
  }

  return server;
}

void GFSMaster::update_leases() {
  uint64_t curr_time = 0;
  std::string new_server;
  server_ts_t new_lease;

  while (1) {
    curr_time = this->_get_ts();

    this->leaseinfo_mutex.lock();
    for (auto const &[chunk, lease] : this->lease_info)
      if (lease.second + LEASE_INTERVAL_MS <= curr_time) {
        new_server = lease.first;
        std::lock_guard<std::mutex> g(this->chunkservers_mutex);
        while (this->chunk_servers[new_server] + (HB_INTERVAL_MS << 1) <= curr_time)
          new_server = this->rand_server(chunk);
        new_lease = std::make_pair(new_server, this->_get_ts());
        if (new_server != lease.first) this->revoke_lease(lease.first, chunk);
        this->set_lease(chunk, new_lease);
        this->lease_info[chunk] = new_lease;
      }
    this->leaseinfo_mutex.unlock();

    std::this_thread::sleep_for(std::chrono::milliseconds(LEASE_INTERVAL_MS));
  }
}

int GFSMaster::revoke_lease(const std::string &server, chunkid_t cid) {
  auto c = grpc::CreateChannel(server, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  grpc::ClientContext ctx;
  gfs::Status r;
  gfs::ChunkID l;

  l.set_id(cid);

  return client->RevokeLease(&ctx, l, &r).ok();
}

int GFSMaster::set_lease(chunkid_t cid, const server_ts_t &lease) {
  auto c = grpc::CreateChannel(lease.first, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  grpc::ClientContext ctx;
  gfs::Status r;
  gfs::Lease l;

  l.set_chunkid(cid);
  l.set_timestamp(lease.second);

  return client->SetLease(&ctx, l, &r).ok();
}

grpc::Status GFSMaster::HeartBeat(grpc::ServerContext *ct,
                                  const gfs::HBPayload *p, gfs::Status *r) {
  uint64_t ts = this->_get_ts();
  std::cout << ts << ": " << p->id() << " heartbeat" << std::endl;

  std::lock_guard<std::mutex> g(this->chunkservers_mutex);
  this->chunk_servers[p->id()] = ts;
  r->set_status(true);
  return grpc::Status::OK;
}

grpc::Status GFSMaster::ChunkReport(grpc::ServerContext *,
                                    const gfs::CRPayload *p, gfs::Status *s) {
  std::cout << "ChunkReport(" << p->id() << "): " << std::endl;

  std::lock_guard<std::mutex> g(this->chunklocs_mutex);
  for (int i = 0; i < p->chunks_size(); ++i)
    this->chunk_locs[p->chunks(i)].insert(p->id());

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

grpc::Status GFSMaster::Remove(grpc::ServerContext *ct,
                               const gfs::FileHandle *fh, gfs::Status *s) {
  std::cout << "Remove(" << fh->filename() << ")" << std::endl;

  std::lock_guard<std::mutex> g(this->chunks_mutex);
  const auto c_it = this->chunks.find(fh->filename());

  if (c_it != this->chunks.end()) {
    for (auto const &chunk : c_it->second) {
      std::lock_guard<std::mutex> l(this->chunklocs_mutex);
      const auto cl_it = this->chunk_locs.find(chunk);
      if (cl_it != this->chunk_locs.end()) {
        for (auto const &server : cl_it->second)
          this->remove_chunk(server, chunk);
        this->chunk_locs.erase(cl_it);
      }
    }
    this->chunks.erase(c_it);
  }

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
  const auto cit = this->chunks.find(file_name);
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
  if (this->chunk_locs.find(cid) != this->chunk_locs.end())
    for (auto const &server : this->chunk_locs[cid]) {
      loc = r->add_loc();
      loc->set_id(server);
      std::lock_guard<std::mutex> g(this->leaseinfo_mutex);
      if (this->lease_info.find(cid) != this->lease_info.end())
        loc->set_primary(this->lease_info[cid].first == server);
    }

  return grpc::Status::OK;
}

grpc::Status GFSMaster::ExtendLease(grpc::ServerContext *c,
                                    const gfs::ELPayload *p, gfs::Lease *l) {
  std::lock_guard<std::mutex> g(this->leaseinfo_mutex);
  uint64_t ts = 0;

  if (this->lease_info.find(p->chunkid()) != this->lease_info.end())
    if (this->lease_info[p->chunkid()].first == p->id()) {
      ts = this->_get_ts();
      this->lease_info[p->chunkid()].second = ts;
      l->set_chunkid(p->chunkid());
      l->set_timestamp(ts);
      return grpc::Status::OK;
    }

  return grpc::Status::CANCELLED;
}

chunkid_t GFSMaster::new_chunkid() {
  std::lock_guard<std::mutex> g(this->chunk_ctr_mutex);
  return this->chunk_ctr++;
}

chunkid_t GFSMaster::new_chunk(const std::string &fname) {
  std::vector<std::string> servers;
  std::string primary_server;
  chunkid_t cid = this->new_chunkid();
  this->get_servers(servers);

  // assign chunk locations
  for (int i = 0; i < RFACTOR; ++i) this->assign_chunk(servers[i], cid);

  // set initial primary lease
  primary_server = servers[std::rand() % (RFACTOR - 1)];
  std::lock_guard<std::mutex> g(this->leaseinfo_mutex);
  this->lease_info[cid] = std::make_pair(primary_server, this->_get_ts());
  this->set_lease(cid, this->lease_info[cid]);

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
