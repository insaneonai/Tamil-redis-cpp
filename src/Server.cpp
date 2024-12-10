#include <iostream>
#include <cstdlib>
#include <string>
#include <vector>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sstream>
#include <unordered_map>
#include <chrono>


std::unordered_map<std::string, std::pair<std::string, long>> mapping;


enum class RespType {
    SimpleString,
    Error,
    Integer,
    BulkString,
    Array,
    Null
};

struct RespValue {
    RespType type;
    std::string str_value;
    long long int_value;
    std::vector<RespValue> array_value;
};

RespValue RespDecoder(const std::string& RespEncoded) {
    RespValue value;

    char first_char = RespEncoded[0]; // Get first character of RespEncoded

    switch (first_char) {
    case '+':
        value.type = RespType::SimpleString;
        value.str_value = RespEncoded.substr(1, RespEncoded.size() - 3); // remove prefix and suffix
        break;
    case '-':
        value.type = RespType::Error;
        value.str_value = RespEncoded.substr(1, RespEncoded.size() - 3);
        break;
    case ':':
        value.type = RespType::Integer;
        value.int_value = std::stoll(RespEncoded.substr(1, RespEncoded.size() - 3));
        break;
    case '$': {
        size_t firstend = RespEncoded.find("\r\n", 1);  // Find the end of the length part
        size_t length = std::stoll(RespEncoded.substr(1, firstend - 1)); // Get the bulk string length

        if (length == -1) {
            value.type = RespType::Null; // Handle Null Bulk String
        }
        else {
            size_t dataStart = firstend + 2; // Move past CRLF
            value.type = RespType::BulkString;
            value.str_value = RespEncoded.substr(dataStart, length); // Extract the bulk string
        }
        break;  // Make sure the break is here to avoid fallthrough
    }
    case '*': {
        size_t firstend = RespEncoded.find("\r\n", 1); // Find the array count
        size_t count = std::stoll(RespEncoded.substr(1, firstend - 1)); // Extract array element count

        value.type = RespType::Array;
        value.array_value.reserve(count); // Reserve space for the array elements

        size_t pos = firstend + 2; // Move past the CRLF to the next item

        for (size_t i = 0; i < count; i++) {
            RespValue element;

            char next_char = RespEncoded[pos]; // Check the type of the next item in the array

            if (next_char == '+') { // Simple String
                element.type = RespType::SimpleString;
                size_t end_pos = RespEncoded.find("\r\n", pos);
                element.str_value = RespEncoded.substr(pos + 1, end_pos - pos - 2); // Handle CRLF correctly
                pos = end_pos + 2; // Move past CRLF
            }
            else if (next_char == '-') { // Error
                element.type = RespType::Error;
                size_t end_pos = RespEncoded.find("\r\n", pos);
                element.str_value = RespEncoded.substr(pos + 1, end_pos - pos - 2); // Handle CRLF correctly
                pos = end_pos + 2; // Move past CRLF
            }
            else if (next_char == ':') { // Integer
                element.type = RespType::Integer;
                size_t end_pos = RespEncoded.find("\r\n", pos);
                element.int_value = std::stoll(RespEncoded.substr(pos + 1, end_pos - pos - 2)); // Handle CRLF correctly
                pos = end_pos + 2; // Move past CRLF
            }
            else if (next_char == '$') { // Bulk String
                size_t length_end = RespEncoded.find("\r\n", pos);
                size_t length = std::stoll(RespEncoded.substr(pos + 1, length_end - pos - 1)); // Get the bulk string length

                if (length == -1) { // Null Bulk String
                    element.type = RespType::Null;
                    pos = length_end + 2; // Move past CRLF
                }
                else {
                    size_t data_start = length_end + 2; // Skip CRLF
                    element.type = RespType::BulkString;
                    element.str_value = RespEncoded.substr(data_start, length); // Extract the bulk string
                    pos = data_start + length + 2; // Move past data and CRLF
                }
            }
            else {
                throw std::runtime_error("Invalid RESP format"); // In case of an invalid format
            }

            value.array_value.push_back(element); // Add element to the array
        }
        break;  // Ensure break here to terminate the case block properly
    }
    default:
        throw std::runtime_error("Invalid RESP format"); // Default case for unhandled RESP types
    }

    return value; // Return the parsed value
}

const char* RespEncoder(const char* str) {
    size_t len = strlen(str);

    std::ostringstream oss;

    oss << "$" << len << "\r\n" << str << "\r\n";

    static std::string result;
    result = oss.str();

    return result.c_str();
}


void handle(int client_fd) {
    std::vector<char> buff(5000);

    while (true) {
        ssize_t stream = recv(client_fd, buff.data(), buff.size() - 1, 0); // Read more bytes

        if (stream < 0) {
            std::cerr << "Error occurred while receiving data\n";
            break;
        }
        else if (stream == 0) {
            std::cout << "Client connection closed\n";
            break;
        }
        else {
            buff[stream] = '\0'; // Null-terminate the string for safety
            std::string data(buff.data(), stream);


            RespValue res = RespDecoder(data);

            if (res.type == RespType::Array) {
                const char* encoded;
                for (int i = 0; i < res.array_value.size(); i++) {
                    if (res.array_value[i].type == RespType::SimpleString || res.array_value[i].type == RespType::BulkString) {
                        if (res.array_value[i].str_value == "VANAKAMDAMAPLA") {
                            encoded = RespEncoder(res.array_value[i + 1].str_value.c_str());
                            send(client_fd, encoded , strlen(encoded), 0);
                        }
                        if (res.array_value[i].str_value == "SET") {
                            if (res.array_value.size() == 1) {
                                encoded = RespEncoder("Vaida P**M**ne: keyum illa valueum illa enatha set panna?");

                                send(client_fd, encoded, strlen(encoded), 0);
                            }
                            else if (res.array_value.size() == 2) {
                                encoded = RespEncoder("Vaida P**M**ne: Enna Pangu.. key mattum vachu naan naakku valikanumaa? value kudu da venna...");

                                send(client_fd, encoded, strlen(encoded), 0);
                            }
                            else if (res.array_value.size() == 3) {
                                encoded = RespEncoder("Payan Pudichitaan: Set agidichu Maamey....");

                                // std::cout << res.array_value[i + 1].str_value << " " << res.array_value[i + 2].str_value << std::endl;

                                mapping[res.array_value[i + 1].str_value] = std::make_pair(res.array_value[i + 2].str_value, 0);
                                
                                send(client_fd, encoded, strlen(encoded), 0);
                            }
                            else {
                                if (res.array_value[i + 3].str_value == "PX") {
                                    if (res.array_value.size() != 5) {
                                        encoded = RespEncoder("Vaida P**M**ne: PX kudutha timeout value kudukanum da sokeruu...");
                                        send(client_fd, encoded, strlen(encoded), 0);
                                    }
                                    else {
                                        encoded = RespEncoder("Payan Pudichitaan: Set agidichu Maamey....");

                                        auto now = std::chrono::system_clock::now();

                                        auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);

                                        auto ms_since_epoch = ms.time_since_epoch();

                                        long ms_count = ms_since_epoch.count() + std::stoll(res.array_value[i + 4].str_value);

                                        mapping[res.array_value[i + 1].str_value] = std::make_pair(res.array_value[i + 2].str_value, ms_count);

                                        send(client_fd, encoded, strlen(encoded), 0);
                                    }
                                }
                                else {
                                    encoded = RespEncoder("Vaida P**M**ne: nee enna solrenu purila docs paru....");
                                    send(client_fd, encoded, strlen(encoded), 0);
                                }
                            }
                        }
                        if (res.array_value[i].str_value == "GET") {

                            if (res.array_value.size() == 1) {
                                encoded = RespEncoder("Vaida P**M**ne: key illama enna search panna?");

                                send(client_fd, encoded, strlen(encoded), 0);
                            }
                            else {
                               // std::cout << "key " << res.array_value[i + 1].str_value;

                                auto now = std::chrono::system_clock::now();

                                auto ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);

                                auto ms_since_epoch = ms.time_since_epoch();

                                long ms_count = ms_since_epoch.count();

                                auto it = mapping.find(res.array_value[i + 1].str_value);

                                if (it != mapping.end() && (it->second.second >= ms_count || it->second.second == 0)) {
                                    encoded = RespEncoder(it->second.first.c_str());
                                    send(client_fd, encoded, strlen(encoded), 0);
                                }
                                else {
                                    encoded = RespEncoder("Vaida P**M**ne: Key illa da...");
                                    send(client_fd, encoded, strlen(encoded), 0);
                                }
                            }
                        }
                    }
                }
            }
            else {
                std::cerr << "Unexpected command received: " << data << "\n";
            }
        }
    }

    close(client_fd);
}



int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
   if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  // // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
     std::cerr << "setsockopt failed\n";
     return 1;
   }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  
  std::cout << "Waiting for a client to connect...\n";

  
  while (true) {
      int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, (socklen_t*)&client_addr_len);
      std::cout << "Client connected\n" << client_fd;
      std::thread newClient(handle, client_fd);

      newClient.detach();
  }

  close(server_fd);

  return 0;
}
