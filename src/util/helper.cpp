#include "helper.h"
#include <regex>

// helper function : get prefix of a string, the filename string is always in the format of prefix/suffix
std::string getPrefix(std::string const& filename) {
  std::regex re(R"(^([^/]+)/[^/]+$)");
  std::smatch match;
  if (std::regex_search(filename, match, re)) {
    return match[1];
  }
  return "";
}

// helper function : get suffix of a string, the filename string is always in the format of prefix/number
std::string getSuffix(std::string const& filename) {
  std::regex re(R"(^[^/]+/([^/]+)$)");
  std::smatch match;
  if (std::regex_search(filename, match, re)) {
    return match[1];
  }
  return "";
}

int getEndOffset(std::string const& filename) {
  std::regex re(R"(^sharedlog/\d+:(\d+)$)");
  std::smatch match;
  if (std::regex_search(filename, match, re)) {
    return std::stoi(match[1]);
  }
  return -1;
}
int getStartOffset(std::string const& filename) {
  std::regex re(R"(^sharedlog/(\d+):\d+$)");
  std::smatch match;
  if (std::regex_search(filename, match, re)) {
    return std::stoi(match[1]);
  }
  return -1;
}

int getNumberBeforeUnderscore(std::string const& filename) {
  // Regular expression to match digits before the first underscore
  std::regex re(R"([^/]+/([0-9]+)_[^/]+$)");
  std::smatch match;
  if (std::regex_search(filename, match, re)) {
    return std::stoi(match[1]);  // The first capturing group is the number before the underscore
  }
  return -1;
}

int getNumberInTheEnd(std::string const& filename) {
  std::regex re(R"(.*(\d+)$)");
  std::smatch match;
  if (std::regex_search(filename, match, re)) {
    return std::stoi(match[1]);
  }
  return -1;
}

// sstable1/e71c7eb6-b20b-4ebe-8a23-965b1269b673202410081038101728405491035205186.sst
int getSSTLayerNumber(std::string const& filename) {
  std::regex re(R"(sstable(\d+)/.*)");
  std::smatch match;
  if (std::regex_search(filename, match, re)) {
    return std::stoi(match[1]);
  }
  return -1;
}

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <chrono>
#include <iomanip>
#include <sstream>

// Function to generate a client-specific fingerprint
std::string generateFingerprint() {
  // Create a UUID generator
  boost::uuids::random_generator generator;
  // Generate a random UUID
  boost::uuids::uuid uuid = generator();

  // Get the current timestamp
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&time_t_now), "%Y%m%d%H%M%S");
  std::string timestamp = ss.str();

  // Combine the random part and the timestamp
  std::stringstream fingerprint;
  fingerprint << uuid << timestamp;
  return fingerprint.str();
}