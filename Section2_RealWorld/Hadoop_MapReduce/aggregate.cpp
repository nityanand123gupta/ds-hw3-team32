// Q7 (Section 2, Q1) - Server Log Analytics, Hadoop Streaming Combiner/Reducer
//
// build: g++ -O2 -std=c++17 -o aggregate aggregate.cpp
//
// One executable serves as BOTH -combiner and -reducer. This is valid
// because every merge here (count/sum via addition, min/max via
// min()/max()) is associative and commutative, and the output line for a
// key is written in exactly the same "key\tvalue" shape it was read in --
// so the reducer can merge combiner output the same way the combiner
// merged mapper output. Hadoop Streaming guarantees keys arrive sorted and
// grouped, so a single left-to-right pass with "flush on key change" is
// enough; no key's data needs to be held in memory once the next key
// starts (except the small per-key accumulator itself).
//
// With -numReduceTasks 1 the reducer's stdout is the complete, globally
// merged aggregate: one line for GLOBAL, one line per server that received
// at least one request, one line per endpoint, one line per 60-second
// interval. finalize.cpp turns that into the assignment's required output
// format (including sorting for Top-K and busiest-interval tie-breaking).
#include <cstdio>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

enum class KeyType { GLOBAL, SERVER, ENDPOINT, INTERVAL, UNKNOWN };

static KeyType classify(const std::string& key) {
    if (key == "GLOBAL") return KeyType::GLOBAL;
    if (key.rfind("SERVER_", 0) == 0) return KeyType::SERVER;
    if (key.rfind("ENDPOINT_", 0) == 0) return KeyType::ENDPOINT;
    if (key.rfind("INTERVAL_", 0) == 0) return KeyType::INTERVAL;
    return KeyType::UNKNOWN;
}

int main() {
    std::ios::sync_with_stdio(false);
    std::string line, curKey;
    bool have = false;
    KeyType curType = KeyType::UNKNOWN;

    long long g_count = 0, g_success = 0, g_bytes = 0;
    long long g_s2 = 0, g_s3 = 0, g_s4 = 0, g_s5 = 0;
    double g_min = std::numeric_limits<double>::infinity();
    double g_max = -std::numeric_limits<double>::infinity();
    double g_sum = 0.0;

    long long p_count = 0;
    double p_sum = 0.0;
    long long p_bytes = 0;

    long long i_count = 0;

    auto reset = [&]() {
        g_count = g_success = g_bytes = g_s2 = g_s3 = g_s4 = g_s5 = 0;
        g_min = std::numeric_limits<double>::infinity();
        g_max = -std::numeric_limits<double>::infinity();
        g_sum = 0.0;
        p_count = 0;
        p_sum = 0.0;
        p_bytes = 0;
        i_count = 0;
    };

    auto flush = [&]() {
        if (!have) return;
        switch (curType) {
            case KeyType::GLOBAL:
                std::printf("GLOBAL\t%lld %lld %.6f %.6f %.6f %lld %lld %lld %lld %lld\n",
                             g_count, g_success, g_min, g_max, g_sum, g_bytes,
                             g_s2, g_s3, g_s4, g_s5);
                break;
            case KeyType::SERVER:
                std::printf("%s\t%lld %.6f\n", curKey.c_str(), p_count, p_sum);
                break;
            case KeyType::ENDPOINT:
                std::printf("%s\t%lld %lld\n", curKey.c_str(), p_count, p_bytes);
                break;
            case KeyType::INTERVAL:
                std::printf("%s\t%lld\n", curKey.c_str(), i_count);
                break;
            default:
                break;
        }
    };

    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;
        size_t tab = line.find('\t');
        if (tab == std::string::npos) continue;
        std::string key = line.substr(0, tab);
        std::string val = line.substr(tab + 1);
        KeyType t = classify(key);

        if (!have || key != curKey) {
            flush();
            curKey = key;
            curType = t;
            have = true;
            reset();
        }

        std::istringstream iss(val);
        switch (t) {
            case KeyType::GLOBAL: {
                long long c, succ, by, s2, s3, s4, s5;
                double mn, mx, sm;
                iss >> c >> succ >> mn >> mx >> sm >> by >> s2 >> s3 >> s4 >> s5;
                g_count += c;
                g_success += succ;
                if (mn < g_min) g_min = mn;
                if (mx > g_max) g_max = mx;
                g_sum += sm;
                g_bytes += by;
                g_s2 += s2; g_s3 += s3; g_s4 += s4; g_s5 += s5;
                break;
            }
            case KeyType::SERVER: {
                long long c;
                double sm;
                iss >> c >> sm;
                p_count += c;
                p_sum += sm;
                break;
            }
            case KeyType::ENDPOINT: {
                long long c, by;
                iss >> c >> by;
                p_count += c;
                p_bytes += by;
                break;
            }
            case KeyType::INTERVAL: {
                long long c;
                iss >> c;
                i_count += c;
                break;
            }
            default:
                break;
        }
    }
    flush();
    return 0;
}
