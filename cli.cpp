#include "timeseries_db.hpp"
#include <iostream>
#include <cstdlib>
#include <iomanip>
#include <random>
#include <chrono>
#include <fstream>
#include <sstream>

void print_help() {
    std::cout << "Usage:\n"
              << "  tsdb_cli insert <symbol> <timestamp> <price> <volume>\n"
              << "  tsdb_cli query <symbol> <start_timestamp> <end_timestamp>\n"
              << "  tsdb_cli last <symbol> <count>\n"
              << "  tsdb_cli benchmark <symbol> <tick_count>\n"
              << "  tsdb_cli import <symbol> <csv_file>\n";
}

// Generate random ticks for benchmark
std::vector<Tick> generate_random_ticks(size_t count) {
    std::vector<Tick> ticks;
    ticks.reserve(count);
    
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<double> price_dist(100.0, 200.0);
    std::uniform_int_distribution<uint64_t> volume_dist(100, 10000);
    
    uint64_t timestamp = std::chrono::duration_cast<std::chrono::seconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    
    for (size_t i = 0; i < count; ++i) {
        ticks.push_back({
            timestamp + i,
            price_dist(gen),
            volume_dist(gen)
        });
    }
    
    return ticks;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        print_help();
        return 1;
    }

    // Default data directory
    const std::string data_dir = "tsdb_data";
    std::string command = argv[1];

    try {
        if (command == "insert") {
            if (argc != 6) {
                print_help();
                return 1;
            }
            std::string symbol = argv[2];
            uint64_t timestamp = std::stoull(argv[3]);
            double price = std::stod(argv[4]);
            uint64_t volume = std::stoull(argv[5]);

            TimeSeriesDB db(data_dir, symbol);
            db.append(timestamp, price, volume);
            db.sync(); // Ensure write is complete
            std::cout << "Inserted tick for " << symbol << std::endl;
        } 
        else if (command == "query") {
            if (argc != 5) {
                print_help();
                return 1;
            }
            std::string symbol = argv[2];
            uint64_t start = std::stoull(argv[3]);
            uint64_t end = std::stoull(argv[4]);

            TimeSeriesDB db(data_dir, symbol);
            auto results = db.query_range(start, end);

            std::cout << "Found " << results.size() << " results:\n";
            for (const auto& [ts, price, vol] : results) {
                std::cout << "Timestamp: " << ts
                          << " Price: " << std::fixed << std::setprecision(2) << price
                          << " Volume: " << vol << std::endl;
            }
        } 
        else if (command == "last") {
            if (argc != 4) {
                print_help();
                return 1;
            }
            std::string symbol = argv[2];
            size_t count = std::stoull(argv[3]);

            TimeSeriesDB db(data_dir, symbol);
            auto results = db.query_last(count);

            std::cout << "Last " << results.size() << " ticks for " << symbol << ":\n";
            for (const auto& [ts, price, vol] : results) {
                std::cout << "Timestamp: " << ts
                          << " Price: " << std::fixed << std::setprecision(2) << price
                          << " Volume: " << vol << std::endl;
            }
        }
        else if (command == "benchmark") {
            if (argc != 4) {
                print_help();
                return 1;
            }
            std::string symbol = argv[2];
            size_t count = std::stoull(argv[3]);
            
            TimeSeriesDB db(data_dir, symbol);
            auto ticks = generate_random_ticks(count);
            
            // Benchmark insert
            auto start_time = std::chrono::high_resolution_clock::now();
            db.append_batch(ticks);
            db.sync();
            auto end_time = std::chrono::high_resolution_clock::now();
            
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time).count();
            
            std::cout << "Inserted " << count << " ticks in " << duration << "ms ("
                      << (count * 1000.0 / duration) << " ticks/second)" << std::endl;
            
            // Benchmark query
            start_time = std::chrono::high_resolution_clock::now();
            auto results = db.query_range(ticks.front().timestamp, ticks.back().timestamp);
            end_time = std::chrono::high_resolution_clock::now();
            
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                end_time - start_time).count();
            
            std::cout << "Retrieved " << results.size() << " ticks in " << duration << "ms ("
                      << (results.size() * 1000.0 / duration) << " ticks/second)" << std::endl;
        }
        else if (command == "import") {
            if (argc != 4) {
                print_help();
                return 1;
            }
            std::string symbol = argv[2];
            std::string csv_file = argv[3];
            
            // Open CSV file
            std::ifstream file(csv_file);
            if (!file.is_open()) {
                std::cerr << "Error: Could not open file " << csv_file << std::endl;
                return 1;
            }
            
            TimeSeriesDB db(data_dir, symbol);
            
            // Parse CSV
            std::string line;
            std::vector<Tick> ticks;
            size_t line_count = 0;
            
            while (std::getline(file, line)) {
                line_count++;
                // Skip header
                if (line_count == 1 && line.find("timestamp") != std::string::npos) {
                    continue;
                }
                
                std::stringstream ss(line);
                std::string token;
                std::vector<std::string> tokens;
                
                while (std::getline(ss, token, ',')) {
                    tokens.push_back(token);
                }
                
                if (tokens.size() < 3) {
                    std::cerr << "Warning: Invalid format at line " << line_count << std::endl;
                    continue;
                }
                
                try {
                    Tick tick;
                    tick.timestamp = std::stoull(tokens[0]);
                    tick.price = std::stod(tokens[1]);
                    tick.volume = std::stoull(tokens[2]);
                    ticks.push_back(tick);
                    
                    // Batch insert every 10,000 ticks
                    if (ticks.size() >= 10000) {
                        db.append_batch(ticks);
                        ticks.clear();
                    }
                }
                catch (const std::exception& e) {
                    std::cerr << "Warning: Could not parse line " << line_count << ": " << e.what() << std::endl;
                }
            }
            
            // Insert remaining ticks
            if (!ticks.empty()) {
                db.append_batch(ticks);
            }
            
            db.sync();
            std::cout << "Imported " << (line_count - 1) << " ticks from " << csv_file << " for symbol " << symbol << std::endl;
        }
        else {
            print_help();
            return 1;
        }
    } 
    catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}