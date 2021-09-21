#include "gfs.h"

#include <openssl/sha.h>
#include <stdio.h>

#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>

std::string sha256sum(const void *payload, size_t len) {
  std::stringstream digest;
  uchar_t buff[SHA256_DIGEST_LENGTH];
  SHA256_CTX context;
  SHA256_Init(&context);
  SHA256_Update(&context, (uchar_t *)payload, len);
  SHA256_Final(buff, &context);
  digest << std::hex << std::setfill('0');
  for (const auto &b : buff) digest << std::setw(2) << (int)b;
  return digest.str();
}

void dump_buff(const char *buff) {
  for (int i = 0; i < CHUNK_SIZE; ++i) printf("%02x ", buff[i]);
  std::cout << std::endl;
}

std::unique_ptr<gfs::Master::Stub> gfs_master_connect() {
  auto master_ip = std::string(BASE_IP) + ":" + BASE_PORT;
  std::cout << "Connecting to: " << master_ip << std::endl;
  auto c = grpc::CreateChannel(master_ip, grpc::InsecureChannelCredentials());
  return gfs::Master::NewStub(c);
}
