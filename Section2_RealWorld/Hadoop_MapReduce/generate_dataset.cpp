// Q7 - reproducible dataset generator
//
// build: g++ -O2 -std=c++17 -o generate_dataset generate_dataset.cpp
// usage: ./generate_dataset <N> <K> <S> <output_file> [seed]
//
// Writes the "N K S" header followed by N records. std::mt19937 with an
// explicit seed (default 42), so a given (N, K, S, seed) always produces a
// byte-identical file.
//
// Per the course clarification, timestamps and entity ids are integers and
// server_id is drawn from [0, S-1]. Response times are the only real-valued
// field and are written with 6 decimals, matching the output precision so a
// value never changes when it is read back.
//
// Shape of the generated data
//   endpoints  : 4*S distinct ids, uniform
//   users      : N/5 distinct ids, uniform (parsed but unused by the analytics)
//   timestamps : uniform over [0, max(60, N/10)) seconds, so there are roughly
//                N/600 sixty-second intervals and the busiest one is well
//                defined rather than being a single-record tie
//   status     : ~60% 2xx, ~13% 3xx, ~20% 4xx, ~7% 5xx
//   response   : 0.5 - 800.0 ms
//   bytes      : 0 - 20000

#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

int main(int argc, char** argv) {
    if (argc < 5 || argc > 6) {
        std::fprintf(stderr, "usage: %s <N> <K> <S> <output_file> [seed]\n", argv[0]);
        return 1;
    }
    long long N = std::atoll(argv[1]);
    long long K = std::atoll(argv[2]);
    long long S = std::atoll(argv[3]);
    const char* outPath = argv[4];
    unsigned seed = argc > 5 ? (unsigned)std::strtoul(argv[5], nullptr, 10) : 42u;

    if (N < 0 || K < 0 || S <= 0) {
        std::fprintf(stderr, "error: need N >= 0, K >= 0, S > 0\n");
        return 1;
    }

    const int statusPool[] = {200, 200, 200, 200, 200, 200, 201, 204,
                              301, 302, 304,
                              400, 401, 403, 404, 404,
                              500, 502, 503};
    const int statusCount = (int)(sizeof(statusPool) / sizeof(statusPool[0]));

    long long numEndpoints = 4 * S;
    long long timeSpan = N / 10 > 60 ? N / 10 : 60;
    long long numUsers = N / 5 > 1 ? N / 5 : 1;

    std::mt19937 rng(seed);
    std::uniform_int_distribution<long long> tsD(0, timeSpan - 1);
    std::uniform_int_distribution<long long> srvD(0, S - 1);
    std::uniform_int_distribution<long long> epD(0, numEndpoints - 1);
    std::uniform_int_distribution<long long> usrD(0, numUsers - 1);
    std::uniform_int_distribution<int> stD(0, statusCount - 1);
    std::uniform_real_distribution<double> rtD(0.5, 800.0);
    std::uniform_int_distribution<int> byD(0, 20000);

    std::FILE* f = std::fopen(outPath, "wb");
    if (!f) {
        std::fprintf(stderr, "error: cannot open output file %s\n", outPath);
        return 1;
    }

    std::string out;
    out.reserve(1 << 20);
    char line[160];
    std::snprintf(line, sizeof(line), "%lld %lld %lld\n", N, K, S);
    out += line;

    for (long long i = 0; i < N; i++) {
        int n = std::snprintf(line, sizeof(line), "%lld %lld %lld %lld %d %.6f %d\n",
                              tsD(rng), srvD(rng), epD(rng), usrD(rng),
                              statusPool[stD(rng)], rtD(rng), byD(rng));
        out.append(line, (size_t)n);
        if (out.size() > (1u << 20)) {
            std::fwrite(out.data(), 1, out.size(), f);
            out.clear();
        }
    }
    if (!out.empty()) std::fwrite(out.data(), 1, out.size(), f);
    std::fclose(f);

    std::fprintf(stderr, "N=%lld K=%lld S=%lld seed=%u endpoints=%lld time_span=%llds -> %s\n",
                 N, K, S, seed, numEndpoints, timeSpan, outPath);
    return 0;
}
