#include <string>
#include <vector>
#ifndef OZONEDB_HELPER_H
#define OZONEDB_HELPER_H

// helper function : get prefix of a string, the filename string is always in the format of prefix/suffix, the prefix may contain /
std::string getPrefix(std::string const& filename);

// helper function : get suffix of a string, the filename string is always in the format of prefix/number
std::string getSuffix(std::string const& filename);

// helper function : get the number in the end of a string, the filename string is always in the format of prefixnumber
int getNumberInTheEnd(std::string const& filename);

int getSSTLayerNumber(std::string const& filename);
// Function to generate a client-specific fingerprint
std::string generateFingerprint();

int getNumberBeforeUnderscore(std::string const& filename);

// Stable, cross-peer compaction task identifier. Two peers that observe the
// same compaction (same input file set + same destination level + same
// in-last-level flag) produce identical strings. Used by the
// [COMPACTION_NEEDED] / [COMPACTION_COMMITTED] log lines so a benchmark
// harness can join "needed" and "committed" events across writer processes.
std::string formatTaskId(std::vector<std::string> const& input_files,
                         int dest_level, bool in_last_level);
#endif