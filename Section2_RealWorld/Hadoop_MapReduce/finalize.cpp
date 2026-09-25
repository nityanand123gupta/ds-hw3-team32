// Q7 (Section 2, Q1) - Server Log Analytics, final formatting stage
//
// build: g++ -O2 -std=c++17 -o finalize finalize.cpp
// usage: ./finalize <aggregated_file> <K> <S>
//
// This is the second (tiny, local) stage of the MapReduce design: by the
// time aggregate.cpp's single reducer has finished, the data left is only
// O(S + distinct_endpoints + distinct_intervals) lines -- typically a few
// thousand at most, regardless of how large N was. Running that through
// another distributed MR pass would add scheduling overhead for no benefit,
// so it is finished locally. It reuses analytics_common.hpp (the same file
// the HW2 sequential/MPI programs use) so Top-K ordering, tie-breaking and
// number formatting are byte-identical to the rest of the assignment.
#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>

#include "analytics_common.hpp"

int main(int argc, char** argv) {
    if (argc != 4) {
        std::fprintf(stderr, "usage: %s <aggregated_file> <K> <S>\n", argv[0]);
        return 1;
    }
    std::ifstream in(argv[1]);
    if (!in) {
        std::fprintf(stderr, "error: cannot open %s\n", argv[1]);
        return 1;
    }
    long long K = std::atoll(argv[2]);
    long long S = std::atoll(argv[3]);

    Stats st((int)S);
    st.min_response_time = 0.0;
    st.max_response_time = 0.0;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        size_t tab = line.find('\t');
        if (tab == std::string::npos) continue;
        std::string key = line.substr(0, tab);
        std::istringstream iss(line.substr(tab + 1));

        if (key == "GLOBAL") {
            long long c, succ, by, s2, s3, s4, s5;
            double mn, mx, sm;
            iss >> c >> succ >> mn >> mx >> sm >> by >> s2 >> s3 >> s4 >> s5;
            st.total = c;
            st.success = succ;
            st.failed = c - succ;
            st.min_response_time = mn;
            st.max_response_time = mx;
            st.sum_response_time = sm;
            st.total_bytes = by;
            st.status2xx = s2; st.status3xx = s3; st.status4xx = s4; st.status5xx = s5;
        } else if (key.rfind("SERVER_", 0) == 0) {
            long long id = std::atoll(key.substr(7).c_str());
            long long c; double sm;
            iss >> c >> sm;
            if (id >= 0 && id < (long long)st.servers.size()) {
                st.servers[(size_t)id].count = c;
                st.servers[(size_t)id].sum_response_time = sm;
            }
        } else if (key.rfind("ENDPOINT_", 0) == 0) {
            long long id = std::atoll(key.substr(9).c_str());
            long long c, by;
            iss >> c >> by;
            EndpointAgg ea;
            ea.count = c;
            ea.bytes = by;
            st.endpoints[(int)id] = ea;
        } else if (key.rfind("INTERVAL_", 0) == 0) {
            long long id = std::atoll(key.substr(9).c_str());
            long long c;
            iss >> c;
            st.intervals[id] = c;
        }
    }

    std::string out = formatOutput(st, (int)K);
    std::fwrite(out.data(), 1, out.size(), stdout);
    return 0;
}
