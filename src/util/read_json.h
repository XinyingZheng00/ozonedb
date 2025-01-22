#ifndef READ_JSON_H
#define READ_JSON_H
#include <map>
#include <string>
#include <vector>

std::string trim(std::string const& str);
std::map<std::string, std::string> parseJSON(std::string const& path);
std::vector<std::string> jsonArrayToVector(std::string& json);
#endif