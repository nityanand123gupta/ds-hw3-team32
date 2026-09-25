// Q7 - shared code for the sequential and MPI server-log analytics programs.
//
// Both programs include this file, so they use the same parser, the same
// aggregation and the same output formatting. That matters for the benchmark
// as much as for correctness: if the sequential baseline parsed the input
// differently from the MPI version, the measured "speedup" would partly be a
// comparison of two parsers rather than of one algorithm at different process
// counts.
#pragma once

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

// ---------------------------------------------------------------- input ----

// Whole-buffer reader. Reads a file, or stdin when path is null, and keeps a
// trailing '\0' so strtod can run straight off the buffer.
struct InputBuffer {
    std::vector<char> data;

    bool loadFile(const char* path) {
        std::FILE* f = std::fopen(path, "rb");
        if (!f) return false;
        std::fseek(f, 0, SEEK_END);
        long long sz = std::ftell(f);
        std::fseek(f, 0, SEEK_SET);
        data.resize((size_t)sz + 1);
        bool ok = (sz == 0) || std::fread(data.data(), 1, (size_t)sz, f) == (size_t)sz;
        std::fclose(f);
        data[(size_t)sz] = '\0';
        return ok;
    }

    // Reads only [begin, end) of the file, then keeps reading until the line
    // straddling `end` is complete. Used by the MPI version so every rank
    // touches only its own slice.
    bool loadRange(const char* path, long long begin, long long end, long long fileSize) {
        std::FILE* f = std::fopen(path, "rb");
        if (!f) return false;
        std::fseek(f, begin, SEEK_SET);
        long long want = end - begin;
        data.clear();
        if (want > 0) {
            data.resize((size_t)want);
            if (std::fread(data.data(), 1, data.size(), f) != data.size()) {
                std::fclose(f);
                return false;
            }
        }
        while (begin + (long long)data.size() < fileSize &&
               (data.empty() || data.back() != '\n')) {
            int c = std::fgetc(f);
            if (c == EOF) break;
            data.push_back((char)c);
        }
        std::fclose(f);
        data.push_back('\0');
        return true;
    }

    void loadStdin() {
        char chunk[1 << 16];
        size_t got;
        while ((got = std::fread(chunk, 1, sizeof(chunk), stdin)) > 0) {
            data.insert(data.end(), chunk, chunk + got);
        }
        data.push_back('\0');
    }
};

// Cursor over an InputBuffer. Every field in this input is either a
// non-negative integer or a decimal, so two primitives cover the whole format.
struct Cursor {
    const char* p;
    const char* end;

    Cursor(const std::vector<char>& b, size_t offset = 0)
        : p(b.data() + offset), end(b.data() + b.size() - 1) {}
    Cursor(const char* begin_, const char* end_) : p(begin_), end(end_) {}

    void skipSpace() {
        while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;
    }
    void skipLine() {
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }
    bool atEnd() {
        skipSpace();
        return p >= end;
    }
    // returns false when there is no number left
    bool readLL(long long& out) {
        skipSpace();
        if (p >= end) return false;
        bool neg = false;
        if (*p == '-') { neg = true; p++; }
        if (p >= end || *p < '0' || *p > '9') return false;
        long long x = 0;
        while (p < end && *p >= '0' && *p <= '9') x = x * 10 + (*p++ - '0');
        out = neg ? -x : x;
        return true;
    }
    bool readDouble(double& out) {
        skipSpace();
        if (p >= end) return false;
        char* stop = nullptr;
        out = std::strtod(p, &stop);
        if (stop == p) return false;
        p = stop;
        return true;
    }
};

// --------------------------------------------------------------- record ----

struct Record {
    long long timestamp;
    int server_id;
    int endpoint_id;
    int status_code;
    double response_time;
    long long bytes_sent;
    // user_id is present in the input but appears nowhere in the output, so it
    // is parsed and dropped rather than stored
};

// reads one whole record; false means the input ran out
inline bool readRecord(Cursor& c, Record& r) {
    long long sid, eid, uid, sc;
    if (!c.readLL(r.timestamp)) return false;
    if (!c.readLL(sid)) return false;
    if (!c.readLL(eid)) return false;
    if (!c.readLL(uid)) return false;
    if (!c.readLL(sc)) return false;
    if (!c.readDouble(r.response_time)) return false;
    if (!c.readLL(r.bytes_sent)) return false;
    r.server_id = (int)sid;
    r.endpoint_id = (int)eid;
    r.status_code = (int)sc;
    return true;
}

// ------------------------------------------------------------ aggregates ----

struct ServerAgg {
    long long count = 0;
    double sum_response_time = 0.0;
};
struct EndpointAgg {
    long long count = 0;
    long long bytes = 0;
};

struct Stats {
    long long total = 0, success = 0, failed = 0;
    double sum_response_time = 0.0;
    double min_response_time = std::numeric_limits<double>::infinity();
    double max_response_time = -std::numeric_limits<double>::infinity();
    long long total_bytes = 0;
    long long status2xx = 0, status3xx = 0, status4xx = 0, status5xx = 0;
    std::vector<ServerAgg> servers;                    // indexed by server_id, size S
    std::unordered_map<int, EndpointAgg> endpoints;    // endpoint ids are unbounded
    std::unordered_map<long long, long long> intervals; // timestamp/60 -> count

    explicit Stats(int S) : servers(S < 0 ? 0 : S) {}
};

inline void accumulate(Stats& st, const Record& r) {
    st.total++;
    if (r.status_code < 400) st.success++;
    else st.failed++;

    st.sum_response_time += r.response_time;
    if (r.response_time < st.min_response_time) st.min_response_time = r.response_time;
    if (r.response_time > st.max_response_time) st.max_response_time = r.response_time;

    st.total_bytes += r.bytes_sent;

    switch (r.status_code / 100) {
        case 2: st.status2xx++; break;
        case 3: st.status3xx++; break;
        case 4: st.status4xx++; break;
        case 5: st.status5xx++; break;
        default: break;
    }

    // the clarification fixes server_id in [0, S-1]; anything outside that is
    // still counted in the global totals but cannot be a top-K server
    if (r.server_id >= 0 && r.server_id < (int)st.servers.size()) {
        ServerAgg& sa = st.servers[r.server_id];
        sa.count++;
        sa.sum_response_time += r.response_time;
    }

    EndpointAgg& ea = st.endpoints[r.endpoint_id];
    ea.count++;
    ea.bytes += r.bytes_sent;

    st.intervals[r.timestamp / 60]++;
}

// ---------------------------------------------------------------- top-K ----

struct ServerRow { int id; long long count; double avg_rt; };
struct EndpointRow { int id; long long count; long long bytes; };

// decreasing count, then increasing id
inline std::vector<ServerRow> topServers(const std::vector<ServerAgg>& servers, int K) {
    std::vector<ServerRow> rows;
    for (int i = 0; i < (int)servers.size(); i++) {
        if (servers[i].count > 0) {
            rows.push_back({i, servers[i].count, servers[i].sum_response_time / servers[i].count});
        }
    }
    std::sort(rows.begin(), rows.end(), [](const ServerRow& a, const ServerRow& b) {
        if (a.count != b.count) return a.count > b.count;
        return a.id < b.id;
    });
    if (K >= 0 && (int)rows.size() > K) rows.resize(K);
    return rows;
}

inline std::vector<EndpointRow> topEndpoints(const std::unordered_map<int, EndpointAgg>& eps, int K) {
    std::vector<EndpointRow> rows;
    rows.reserve(eps.size());
    for (const auto& kv : eps) rows.push_back({kv.first, kv.second.count, kv.second.bytes});
    std::sort(rows.begin(), rows.end(), [](const EndpointRow& a, const EndpointRow& b) {
        if (a.count != b.count) return a.count > b.count;
        return a.id < b.id;
    });
    if (K >= 0 && (int)rows.size() > K) rows.resize(K);
    return rows;
}

// busiest 60-second interval; ties go to the smaller interval id
inline void busiestInterval(const std::unordered_map<long long, long long>& iv,
                            long long& bestId, long long& bestCount) {
    bestId = 0;
    bestCount = 0;
    bool first = true;
    for (const auto& kv : iv) {
        if (first || kv.second > bestCount || (kv.second == bestCount && kv.first < bestId)) {
            bestId = kv.first;
            bestCount = kv.second;
            first = false;
        }
    }
}

// --------------------------------------------------------------- output ----

// All floating-point values are printed with exactly 6 digits after the
// decimal point, per the clarification.
inline void appendFixed6(std::string& s, double v) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "%.6f", v);
    s += buf;
}
inline void appendLL(std::string& s, long long v) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%lld", v);
    s += buf;
}

inline std::string formatOutput(const Stats& st, int K) {
    double avg = st.total > 0 ? st.sum_response_time / st.total : 0.0;
    double mn = st.total > 0 ? st.min_response_time : 0.0;
    double mx = st.total > 0 ? st.max_response_time : 0.0;

    std::string o;
    o.reserve(4096);
    o += "TOTAL_REQUESTS ";      appendLL(o, st.total);       o += '\n';
    o += "SUCCESSFUL_REQUESTS "; appendLL(o, st.success);     o += '\n';
    o += "FAILED_REQUESTS ";     appendLL(o, st.failed);      o += '\n';
    o += "AVERAGE_RESPONSE_TIME "; appendFixed6(o, avg);      o += '\n';
    o += "MIN_RESPONSE_TIME ";   appendFixed6(o, mn);         o += '\n';
    o += "MAX_RESPONSE_TIME ";   appendFixed6(o, mx);         o += '\n';
    o += "TOTAL_BYTES ";         appendLL(o, st.total_bytes); o += '\n';
    o += "STATUS_2XX ";          appendLL(o, st.status2xx);   o += '\n';
    o += "STATUS_3XX ";          appendLL(o, st.status3xx);   o += '\n';
    o += "STATUS_4XX ";          appendLL(o, st.status4xx);   o += '\n';
    o += "STATUS_5XX ";          appendLL(o, st.status5xx);   o += '\n';

    long long bi, bc;
    busiestInterval(st.intervals, bi, bc);
    o += "BUSIEST_INTERVAL ";    appendLL(o, bi); o += ' '; appendLL(o, bc); o += '\n';

    o += "TOP_SERVERS\n";
    for (const ServerRow& r : topServers(st.servers, K)) {
        appendLL(o, r.id); o += ' '; appendLL(o, r.count); o += ' ';
        appendFixed6(o, r.avg_rt); o += '\n';
    }
    o += "TOP_ENDPOINTS\n";
    for (const EndpointRow& r : topEndpoints(st.endpoints, K)) {
        appendLL(o, r.id); o += ' '; appendLL(o, r.count); o += ' ';
        appendLL(o, r.bytes); o += '\n';
    }
    return o;
}

// writes to a file, or to stdout when path is null
inline bool writeOut(const char* path, const std::string& text) {
    if (!path) {
        std::fwrite(text.data(), 1, text.size(), stdout);
        return true;
    }
    std::FILE* f = std::fopen(path, "wb");
    if (!f) return false;
    bool ok = text.empty() || std::fwrite(text.data(), 1, text.size(), f) == text.size();
    std::fclose(f);
    return ok;
}
