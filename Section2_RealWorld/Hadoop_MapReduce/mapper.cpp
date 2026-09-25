// Q7 (Section 2, Q1) - Server Log Analytics, Hadoop Streaming Mapper
//
// build: g++ -O2 -std=c++17 -o mapper mapper.cpp
//
// Reads one log record per line (the "N K S" header is stripped before the
// body is handed to Hadoop; N/K/S travel to the reducer/finalizer via job
// arguments instead). For every record the mapper emits four kinds of
// partial-aggregate key/value pairs, one per output line:
//
//   GLOBAL          -> count success min max sum bytes s2xx s3xx s4xx s5xx
//   SERVER_<id>     -> count sum_response_time
//   ENDPOINT_<id>   -> count bytes
//   INTERVAL_<id>   -> count
//
// Every value is already in "reduced" shape (a single-record partial), so
// the exact same merge logic in aggregate.cpp can be reused unmodified as
// both -combiner and -reducer (see aggregate.cpp for why that is safe).
#include <cstdio>
#include <iostream>
#include <sstream>
#include <string>

int main() {
    std::ios::sync_with_stdio(false);
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;
        std::istringstream iss(line);
        long long ts, sid, eid, uid, sc, bytes;
        double rt;
        if (!(iss >> ts >> sid >> eid >> uid >> sc >> rt >> bytes)) continue;

        int success = (sc < 400) ? 1 : 0;
        int s2 = 0, s3 = 0, s4 = 0, s5 = 0;
        switch (sc / 100) {
            case 2: s2 = 1; break;
            case 3: s3 = 1; break;
            case 4: s4 = 1; break;
            case 5: s5 = 1; break;
            default: break;
        }

        std::printf("GLOBAL\t1 %d %.6f %.6f %.6f %lld %d %d %d %d\n",
                     success, rt, rt, rt, bytes, s2, s3, s4, s5);
        std::printf("SERVER_%lld\t1 %.6f\n", sid, rt);
        std::printf("ENDPOINT_%lld\t1 %lld\n", eid, bytes);
        std::printf("INTERVAL_%lld\t1\n", ts / 60);
    }
    return 0;
}
