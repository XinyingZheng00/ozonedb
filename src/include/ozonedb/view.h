#ifndef VIEW_H
#define VIEW_H

#include <deque>
#include <string>
#include <unordered_map>

namespace ozonedb {

class View {
 public:
  std::unordered_map<std::string, std::deque<std::string>> storage_layout;
  std::unordered_map<std::string, std::pair<std::string, std::string>> key_range;
  std::unordered_map<std::string, size_t> file_size;
  std::string current_log_tail;

  int getFileNumber(std::string const& prefix);

  std::deque<std::string> getWithPrefix(std::string const& prefix);

  std::pair<std::string, std::string> getKeyRange(std::string const& file_name);

  size_t getFileSize(std::string const& file_name);

  std::string getCurrentLogTail() { return current_log_tail; }
};
}  // namespace ozonedb
#endif  // VIEW_H