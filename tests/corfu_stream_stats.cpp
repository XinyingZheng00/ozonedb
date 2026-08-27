// Read-only diagnostic for one OzoneDB Corfu stream.
//
// Usage:
//   corfu_stream_stats <corfu-endpoint> <corfu-bridge.jar> <stream-name>
//
// Replays the stream through CorfuDBStorage (which prints the replay line,
// including how many peer APPENDs were dropped because a SEAL preceded
// them), then walks metadata.log in address order and simulates the View
// rollforward the way MetadataLogHandler applies COMPACT records: a COMPACT
// applies only when its first input is at the front of its level's deque.
// Prints every record, then the COMPACTs that never applied, and whether
// another COMPACT with the same inputs did (a duplicate execution, no
// data lost) or not (the outputs hold records no View can reach).
#include "corfu_storage.h"
#include "protobuf/record.pb.h"
#include "protobuf_serializer.h"
#include <algorithm>
#include <deque>
#include <iostream>
#include <map>
#include <set>
#include <string>
#include <vector>

using namespace ozonedb;

namespace {
std::string prefixOf(std::string const& file) {
  auto p = file.find('/');
  return p == std::string::npos ? file : file.substr(0, p);
}

std::string shortName(std::string const& file) {
  // "sstable1/<fingerprint><nanos>.sst" -> "sstable1/…<last 8 of stem>.sst"
  if (file.size() <= 24) return file;
  return prefixOf(file) + "/…" + file.substr(file.size() - 12);
}

std::string joinInputs(OperationRecord const& r) {
  std::string s;
  for (auto const& f : r.input_files()) s += (s.empty() ? "" : "+") + shortName(f);
  return s;
}
}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " <endpoint> <bridge-jar> <stream>\n";
    return 1;
  }
  CorfuDBStorage storage(argv[1], argv[2], "-Xmx4g", argv[3], "/tmp/corfu_stream_stats/");
  std::cout << "[stats] applied_addr=" << storage.lastAppliedAddr()
            << " dropped_above_seal=" << storage.droppedAboveSeal()
            << " dropped_above_seal_KB=" << (storage.droppedAboveSealBytes() >> 10) << "\n";

  unsigned char* buf = nullptr;
  size_t size = 0;
  if (storage.read("metadata.log", buf, size) != Status::kSuccess || buf == nullptr) {
    std::cerr << "[stats] no metadata.log on this stream\n";
    return 2;
  }
  std::vector<google::protobuf::Message*> msgs;
  protobuf::deserializeMessages(buf, size, msgs, []() -> google::protobuf::Message* { return new OperationRecord(); });
  delete[] buf;
  std::cout << "[stats] metadata.log " << size << " bytes, " << msgs.size() << " records\n";

  std::map<std::string, std::deque<std::string>> layout;
  std::string tail;
  std::vector<OperationRecord*> pending;
  std::map<std::string, int> applied_input_sets;  // "a+b" -> count applied
  int applied = 0, logcreates = 0, compacts = 0, pos = 0;

  auto try_apply = [&](OperationRecord* r) -> bool {
    std::string in_prefix = prefixOf(r->input_files(0));
    auto& lvl = layout[in_prefix];
    if (r->compact_in_last_level()) {
      int index = -1;
      for (auto const& in : r->input_files()) {
        auto it = std::find(lvl.begin(), lvl.end(), in);
        if (it == lvl.end()) return false;
      }
      auto first = std::find(lvl.begin(), lvl.end(), r->input_files(0));
      index = static_cast<int>(first - lvl.begin());
      for (auto const& in : r->input_files()) lvl.erase(std::find(lvl.begin(), lvl.end(), in));
      for (int i = 0; i < r->output_file_size(); ++i) lvl.insert(lvl.begin() + std::min<int>(index + i, lvl.size()), r->output_file(i));
      return true;
    }
    if (lvl.empty() || lvl.front() != r->input_files(0)) return false;
    for (int i = 0; i < r->input_files_size(); ++i) lvl.pop_front();
    for (auto const& out : r->output_file()) layout[prefixOf(out)].push_back(out);
    return true;
  };

  for (auto* m : msgs) {
    auto* r = static_cast<OperationRecord*>(m);
    ++pos;
    if (r->op_type() == OperationRecord::LOGCREATE) {
      ++logcreates;
      std::string const& in = r->input_files(0);
      std::string const& out = r->output_file(0);
      bool ok = tail.empty() || tail == in;
      std::cout << "#" << pos << " LOGCREATE " << (in.empty() ? "-" : in) << " -> " << out
                << (ok ? "" : "  (ignored: tail is " + tail + ")") << "\n";
      if (ok) {
        layout[prefixOf(out)].push_back(out);
        tail = out;
      }
      continue;
    }
    ++compacts;
    std::string ins = joinInputs(*r);
    bool ok = try_apply(r);
    std::cout << "#" << pos << " COMPACT " << ins << " -> " << r->output_file_size() << " output(s)"
              << (r->compact_in_last_level() ? " [last level]" : "")
              << (ok ? "" : "  BUFFERED (input not at front)") << "\n";
    if (ok) {
      ++applied;
      ++applied_input_sets[ins];
      // Retry buffered records until none applies (what the rollforward loop does).
      bool progress = true;
      while (progress) {
        progress = false;
        for (auto it = pending.begin(); it != pending.end(); ++it) {
          if (try_apply(*it)) {
            std::cout << "   … buffered COMPACT " << joinInputs(**it) << " now applied\n";
            ++applied;
            ++applied_input_sets[joinInputs(**it)];
            pending.erase(it);
            progress = true;
            break;
          }
        }
      }
    } else {
      pending.push_back(r);
    }
  }

  std::cout << "\n[stats] LOGCREATE=" << logcreates << " COMPACT=" << compacts
            << " applied=" << applied << " never_applied=" << pending.size() << "\n";
  std::cout << "[stats] final View:";
  for (auto const& kv : layout) std::cout << " " << kv.first << "=" << kv.second.size();
  std::cout << " (tail " << tail << ")\n";
  int dup = 0, lost = 0;
  for (auto* r : pending) {
    std::string ins = joinInputs(*r);
    bool covered = applied_input_sets.count(ins) > 0;
    if (covered) ++dup; else ++lost;
    std::cout << "[stats] never applied: COMPACT " << ins << " -> "
              << (r->output_file_size() ? shortName(r->output_file(0)) : "-")
              << (covered ? "  (duplicate of an applied task: no data lost)"
                          : "  (NOT covered: its outputs are unreachable)") << "\n";
  }
  std::cout << "[stats] never_applied duplicates=" << dup << " uncovered=" << lost << "\n";
  for (auto* m : msgs) delete m;
  return 0;
}
