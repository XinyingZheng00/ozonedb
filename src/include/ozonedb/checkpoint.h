#ifndef OZONEDB_CHECKPOINT_H
#define OZONEDB_CHECKPOINT_H
#include "storage.h"
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ozonedb {
namespace checkpoint {

/**
 * @brief The exact live state of the shared log at one global address.
 *
 * A State at covered_addr holds what a process has after it applied every
 * log entry with address <= covered_addr and nothing above it: the bytes of
 * every file that was not removed, the sealed files with their SEAL
 * address, and the names of removed files. A joiner that restores this
 * State and then applies entries covered_addr+1.. ends in the same state as
 * a joiner that replayed the log from address 0. That equality is the
 * invariant every writer and reader of a checkpoint relies on.
 *
 * The bytes of removed files are not here on purpose. Compaction moved
 * them into SSTables on the object store before it removed the file, so
 * the checkpoint loses nothing, and dropping them is what bounds a join.
 */
struct State {
  long covered_addr = -1;
  long prev_covered_addr = -1;
  std::unordered_map<std::string, std::vector<unsigned char>> files;
  // file -> SEAL address (-1 when the address is unknown). Every sealed
  // file is here, including one that never received an APPEND.
  std::unordered_map<std::string, long> sealed;
  std::unordered_set<std::string> removed;
  std::string creator;

  size_t liveBytes() const;
};

// Object keys, relative to the store's own prefix.
//   <dir>/LATEST                  text: "<covered_addr>\n", last line wins
//   <dir>/<covered_addr>/manifest CheckpointManifest proto
//   <dir>/<covered_addr>/files/<file name>
std::string latestKey(std::string const& dir);
std::string manifestKey(std::string const& dir, long covered_addr);
std::string fileKey(std::string const& dir, long covered_addr, std::string const& name);

/**
 * @brief Write one checkpoint: every file object, then the manifest, then
 * LATEST. LATEST is written last, so a crash before it leaves an orphan
 * directory and no visible change. Returns kFailure with LATEST untouched
 * when any earlier object fails.
 */
Status write(Storage& store, std::string const& dir, State const& state);

/**
 * @brief Read LATEST. found is false (and the status kSuccess) when the
 * store holds no checkpoint.
 */
Status readLatestAddr(Storage& store, std::string const& dir, long& covered_addr, bool& found);

/**
 * @brief Read the checkpoint at covered_addr into state. With with_files
 * false only the manifest is read (names, sizes, sealed/removed sets,
 * prev_covered_addr); state.files stays empty.
 */
Status read(Storage& store, std::string const& dir, long covered_addr, State& state,
            bool with_files = true);

/** @brief Read LATEST, then the checkpoint it names. */
Status readLatest(Storage& store, std::string const& dir, State& state, bool& found);

/**
 * @brief Delete one checkpoint's objects, manifest last. Never touches
 * LATEST: the caller only removes checkpoints older than the one LATEST
 * names.
 */
Status remove(Storage& store, std::string const& dir, long covered_addr);

}  // namespace checkpoint
}  // namespace ozonedb
#endif  // OZONEDB_CHECKPOINT_H
