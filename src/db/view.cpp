#include "view.h"
namespace ozonedb {

// get file number in the view
int View::getFileNumber(std::string const& prefix) {
  return storage_layout[prefix].size();
}

std::deque<std::string> View::getWithPrefix(std::string const& prefix) {
  return storage_layout[prefix];
}

std::pair<std::string, std::string> View::getKeyRange(std::string const& file_name) {
  return key_range[file_name];
}

size_t View::getFileSize(std::string const& file_name) {
  return file_size[file_name];
}
}  // namespace ozonedb