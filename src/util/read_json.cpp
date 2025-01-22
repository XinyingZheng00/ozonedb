#include "read_json.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sstream>

// Function to trim leading and trailing whitespaces from a string
std::string trim(std::string const& str) {
  size_t first = str.find_first_not_of(' ');
  size_t last = str.find_last_not_of(' ');
  if (first == std::string::npos || last == std::string::npos)
    return "";
  return str.substr(first, last - first + 1);
}

// Function to parse key-value pairs from JSON
std::map<std::string, std::string> parseJSON(std::string const& path) {
  std::map<std::string, std::string> result;
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    std::cerr << "Failed to open JSON file." << std::endl;
    return result;
  }

  // Read the entire contents of the file into a string
  std::string json_str((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());

  std::istringstream iss(json_str);

  std::string line;
  while (std::getline(iss, line)) {
    line = trim(line);
    if (line.empty() || line.front() == '{' || line.front() == '}')
      continue;
    size_t comma_pos = line.find_last_of(',');
    if (comma_pos != std::string::npos) {
      line = line.substr(0, comma_pos);
    }
    size_t colon_pos = line.find(':');
    if (colon_pos != std::string::npos) {
      std::string key = trim(line.substr(0, colon_pos));
      std::string value = trim(line.substr(colon_pos + 1));
      // Remove quotes from the key, value
      key.erase(std::remove_if(key.begin(), key.end(), [](char c) { return c == '"'; }), key.end());
      value.erase(std::remove_if(value.begin(), value.end(), [](char c) { return c == '"'; }), value.end());
      result[key] = value;
    }
  }

  return result;
}

std::vector<std::string> jsonArrayToVector(std::string & json) {
  std::vector<std::string> vec;
  std::string delimiter = ",";
  size_t pos = 0;
  std::string token;

  // Trim and remove the surrounding brackets if present
  json = trim(json);
  if (json[0] == '[') {
    json.erase(0, 1);  // Remove '['
  }
  if (json[json.length() - 1] == ']') {
    json.erase(json.length() - 1, 1);  // Remove ']'
  }

  // Trim again after removing brackets
  json = trim(json);

  // If no delimiter is found and json is not empty, it must be a single element
  if (json.find(delimiter) == std::string::npos) {
    if (!json.empty()) {
      vec.push_back(json);  // Single element case
    }
    return vec;
  }

  // Handle multiple elements in the array
  while ((pos = json.find(delimiter)) != std::string::npos) {
    token = json.substr(0, pos);
    token = trim(token);  // Trim each token
    vec.push_back(token);
    json.erase(0, pos + delimiter.length());
  }

  // If there's a last remaining token, add it to the vector
  if (!json.empty()) {
    json = trim(json);
    vec.push_back(json);
  }

  return vec;
}