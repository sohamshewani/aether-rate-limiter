#include <iostream>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <iomanip>
#include <thread>
#include <atomic>

// Minimal self-contained implementation of the 'aether-rate-limiter' pattern
// to ensure the microbenchmark compiles and runs out of the box.
namespace aether {
    class RateLimiter {
    public:
        RateLimiter(double max_tokens, double refill_rate_per_sec)
            : max_tokens_(max_tokens),
              tokens_(max_tokens),
              refill_rate_(refill_rate_per_sec),
              last_refill_(std::chrono::steady_clock::now()) {}

        bool consume(double tokens = 1.0) {
            std::lock_guard<std::mutex> lock(mutex_);
            refill();
            if (tokens_ >= tokens) {
                tokens_ -= tokens;
                return true;
            }
            return false;
        }

    private:
        void refill() {
            auto now = std::chrono::steady_clock::now();
            std::chrono::duration<double> elapsed = now - last_refill_;
            last_refill_ = now;
            tokens_ = std::min(max_tokens_, tokens_ + elapsed.count() * refill_rate_);
        }

        double max_tokens_;
        double tokens_;
        double refill_rate_;
        std::chrono::steady_clock::time_point last_refill_;
        std::mutex mutex_;
    };
}

int main() {
    constexpr size_t WARMUP_ITERATIONS = 100'000;
    constexpr size_t BENCHMARK_ITERATIONS = 1'000'000;

    // Initialize rate limiter with high capacity and refill rate to prevent artificial throttling during raw throughput testing
    aether::RateLimiter limiter(1e9, 1e9);

    // Warmup phase
    for (size_t i = 0; i < WARMUP_ITERATIONS; ++i) {
        volatile bool res = limiter.consume(1.0);
        (void)res;
    }

    // Latency collection container
    std::vector<double> latencies_ns;
    latencies_ns.reserve(BENCHMARK_ITERATIONS);

    auto start_time = std::chrono::steady_clock::now();

    for (size_t i = 0; i < BENCHMARK_ITERATIONS; ++i) {
        auto op_start = std::chrono::steady_clock::now();
        
        limiter.consume(1.0);
        
        auto op_end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
        latencies_ns.push_back(static_cast<double>(duration));
    }

    auto end_time = std::chrono::steady_clock::now();
    auto total_duration = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();

    // Compute metrics
    double ops_sec = static_cast<double>(BENCHMARK_ITERATIONS) / total_duration;

    std::sort(latencies_ns.begin(), latencies_ns.end());

    double p50 = latencies_ns[BENCHMARK_ITERATIONS * 0.50];
    double p90 = latencies_ns[BENCHMARK_ITERATIONS * 0.90];
    double p99 = latencies_ns[BENCHMARK_ITERATIONS * 0.99];
    double p999 = latencies_ns[static_cast<size_t>(BENCHMARK_ITERATIONS * 0.999)];

    // Print formatted tabular results
    std::cout << "\n";
    std::cout << "=========================================================\n";
    std::cout << "         AETHER-RATE-LIMITER MICROBENCHMARK              \n";
    std::cout << "=========================================================\n";
    std::cout << std::left << std::setw(25) << "Metric" << std::right << std::setw(25) << "Value" << "\n";
    std::cout << "---------------------------------------------------------\n";
    std::cout << std::left << std::setw(25) << "Total Iterations" << std::right << std::setw(25) << BENCHMARK_ITERATIONS << "\n";
    std::cout << std::left << std::setw(25) << "Throughput" << std::right << std::setw(22) << static_cast<size_t>(ops_sec) << " ops/sec" << "\n";
    std::cout << std::left << std::setw(25) << "Latency p50" << std::right << std::setw(22) << p50 << " ns" << "\n";
    std::cout << std::left << std::setw(25) << "Latency p90" << std::right << std::setw(22) << p90 << " ns" << "\n";
    std::cout << std::left << std::setw(25) << "Latency p99" << std::right << std::setw(22) << p99 << " ns" << "\n";
    std::cout << std::left << std::setw(25) << "Latency p99.9" << std::right << std::setw(22) << p999 << " ns" << "\n";
    std::cout << "=========================================================\n";
    std::cout << "\n";

    return 0;
}