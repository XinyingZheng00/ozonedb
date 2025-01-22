#include <string>
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
#endif