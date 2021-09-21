#include "gfsclient.h"

#include <iostream>

#include "gfs.h"

file_handle_t* GFSClient::open(const std::string& file_name, fmode_t mode) {
  file_handle_t* f = nullptr;
  grpc::ClientContext c;
  gfs::OpenPayload p;
  gfs::FileHandle fh;

  p.set_id(this->ip);
  p.set_filename(file_name);
  p.set_mode(mode);

  auto status = this->master->Open(&c, p, &fh);
  if (status.ok()) f = new file_handle_t(fh.filename(), mode, fh.ptr());

  return f;
}

void GFSClient::close(file_handle_t* fh) {
  chunk_handle_t* ch;
  std::cout << "Close(" << fh->file_name << ")" << std::endl;
  if (fh && (ch = this->request_chunk(fh->file_name, fh->ptr))) {
    std::lock_guard<std::mutex> g(fh->fh_mutex);
    size_t bleft = (CHUNK_SIZE - (fh->ptr % CHUNK_SIZE));
    if (bleft) memset(fh->buff + (fh->ptr % CHUNK_SIZE), 0, bleft);
    this->flush(ch, fh->buff);
    delete ch;
  }
}

int GFSClient::write(file_handle_t* fh, const void* buff, size_t nbytes) {
  chunk_handle_t* ch = nullptr;
  size_t bytes_left = CHUNK_SIZE - (fh->ptr % CHUNK_SIZE), nwrite = 0;
  std::cout << "Write(" << buff << ", " << nbytes << ")" << std::endl;

  if (fh) {
    // fill & flush remainder of current chunk
    if (nbytes >= bytes_left &&
        (ch = this->request_chunk(fh->file_name, fh->ptr))) {
      std::lock_guard<std::mutex> g(fh->fh_mutex);
      memcpy(fh->buff + (fh->ptr % CHUNK_SIZE), buff, bytes_left);
      this->flush(ch, fh->buff);
      fh->ptr += bytes_left;
      nwrite += bytes_left;
      nbytes -= bytes_left;
      delete ch;
    }

    if (nbytes > 0) {
      if (nbytes >= CHUNK_SIZE)
        return nwrite + this->write(fh, (char*)buff + nbytes, nbytes);

      bytes_left = nbytes > CHUNK_SIZE ? CHUNK_SIZE : nbytes;
      memcpy(fh->buff, buff, nbytes);
      fh->ptr += nbytes;
      nwrite += nbytes;
    }
  }

  return nwrite;
}

int GFSClient::flush(const chunk_handle_t* ch, const void* buff) {
  if (ch && buff) {
    for (const auto& x : ch->loc)
      for (int i = 0; i < RETRY_SEND; ++i)
        if (!this->send_chunk(x.first, ch->cid, buff))
          break;
        else if (i == RETRY_SEND - 1)
          return -1;
  }
  return 0;
}

int GFSClient::send_chunk(const std::string& ip_addr, chunkid_t cid,
                          const void* buff) {
  auto c = grpc::CreateChannel(ip_addr, grpc::InsecureChannelCredentials());
  auto client = std::move(gfs::CServer::NewStub(c));

  gfs::Status r;
  gfs::Chunk chunk;
  grpc::ClientContext ctx;

  chunk.set_id(cid);
  chunk.set_data(buff, CHUNK_SIZE);
  chunk.set_checksum(sha256sum(buff, CHUNK_SIZE));

  return client->SendChunk(&ctx, chunk, &r).ok();
}

chunk_handle_t* GFSClient::request_chunk(const std::string& file_name,
                                         uint64_t offset) {
  chunk_handle_t* ch = nullptr;
  grpc::ClientContext c;
  gfs::FileHandle fh;
  gfs::RCResp r;

  fh.set_filename(file_name);
  fh.set_ptr(offset);

  std::cout << "request_chunk(" << file_name << ", " << offset << ") = ";

  if (this->master->RequestChunk(&c, fh, &r).ok()) {
    ch = new chunk_handle_t;
    ch->cid = r.chunkid();
    for (int i = 0; i < r.loc_size(); ++i) {
      std::cout << "<" << r.loc(i).id() << ", " << r.loc(i).primary() << "> ";
      ch->loc[i] = std::make_pair(r.loc(i).id(), r.loc(i).primary());
    }
    std::cout << std::endl;
  }

  return ch;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " PORT" << std::endl;
    return 1;
  }

  auto ip_addr = std::string(BASE_IP) + ":" + argv[1];
  GFSClient client(ip_addr);

  file_handle_t* fh = client.open("file.txt", WRITE);
  if (fh) {
    std::cout << "File handle: " << fh->file_name << ":" << fh->ptr
              << std::endl;
  }

  const int buff_size = (CHUNK_SIZE << 2) + 8;
  char buff[buff_size];
  std::srand(std::time(0));

  for (int i = 0; i < buff_size; ++i) buff[i] = std::rand();

  int nbytes = client.write(fh, buff, buff_size);

  std::cout << "Wrote " << nbytes << " bytes" << std::endl;
  std::cout << "file ptr = " << fh->ptr << std::endl;

  client.close(fh);

  return 0;
}
