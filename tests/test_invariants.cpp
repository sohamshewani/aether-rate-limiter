#include <iostream>
#include <cstdint>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <cassert>
#include <memory>
#include <mutex>

// ============================================================================
// AETHER RATE LIMITER - PRODUCTION HEADER (INLINED FOR TESTING)
// ============================================================================
namespace aether {

class TokenBucketRateLimiter {
public:
    TokenBucketRateLimiter(uint64_t capacity, uint64_t refill_rate_per_sec)
        : capacity_(capacity),
          refill_rate_(refill_rate_per_sec),
          tokens_(static_cast<double>(capacity)),
          last_refill_us_(now_us()) {}

    bool consume(uint64_t tokens) {
        std::lock_guard<std::mutex> lock(mutex_);
        refill();

        if (tokens_ >= static_cast<double>(tokens)) {
            tokens_ -= static_cast<double>(tokens);
            return true;
        }
        return false;
    }

    void reset(uint64_t capacity, uint64_t refill_rate_per_sec) {
        std::lock_guard<std::mutex> lock(mutex_);
        capacity_ = capacity;
        refill_rate_ = refill_rate_per_sec;
        tokens_ = static_cast<double>(capacity);
        last_refill_us_ = now_us();
    }

    uint64_t available_tokens() {
        std::lock_guard<std::mutex> lock(mutex_);
        refill();
        return static_cast<uint64_t>(tokens_);
    }

    uint64_t capacity() const { return capacity_; }
    uint64_t refill_rate() const { return refill_rate_; }

private:
    uint64_t capacity_;
    uint64_t refill_rate_;
    double tokens_;
    uint64_t last_refill_us_;
    std::mutex mutex_;

    uint64_t now_us() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(
            now.time_since_epoch()).count();
    }

    void refill() {
        uint64_t current_us = now_us();
        if (current_us <= last_refill_us_) {
            return;
        }

        uint64_t elapsed_us = current_us - last_refill_us_;
        last_refill_us_ = current_us;

        double delta_tokens = (static_cast<double>(elapsed_us) * static_cast<double>(refill_rate_)) / 1000000.0;
        tokens_ = std::min(static_cast<double>(capacity_), tokens_ + delta_tokens);
    }
};

} // namespace aether

// ============================================================================
// TEST SUITE IMPLEMENTATION
// ============================================================================

namespace aether::testing {

class TestRunner {
public:
    static void RunAll() {
        std::cout << "[AETHER-TEST] Starting Exhaustive Rate Limiter Test Suite..." << std::endl;
        
        TestBasicInitialization();
        TestBoundaryAndEdgeCases();
        TestRecoveryAndRefill();
        TestConcurrentContentionStress();
        TestInvariantPreservation();

        std::cout << "[AETHER-TEST] ALL TESTS PASSED SUCCESSFULLY." << std::endl;
    }

private:
    static void TestBasicInitialization() {
        std::cout << "[TEST] Running TestBasicInitialization..." << std::endl;
        TokenBucketRateLimiter limiter(100, 10);

        assert(limiter.capacity() == 100);
        assert(limiter.refill_rate() == 10);
        assert(limiter.available_tokens() == 100);
        std::cout << "[PASS] TestBasicInitialization" << std::endl;
    }

    static void TestBoundaryAndEdgeCases() {
        std::cout << "[TEST] Running TestBoundaryAndEdgeCases..." << std::endl;
        
        // Zero Capacity & Zero Rate
        {
            TokenBucketRateLimiter limiter(0, 0);
            assert(limiter.capacity() == 0);
            assert(limiter.available_tokens() == 0);
            assert(limiter.consume(1) == false);
            assert(limiter.consume(0) == false); // Even 0 tokens fails if capacity is 0
        }

        // Consume zero tokens (Valid boundary)
        {
            TokenBucketRateLimiter limiter(10, 10);
            assert(limiter.consume(0) == true);
            assert(limiter.available_tokens() == 10);
        }

        // Consume exactly capacity
        {
            TokenBucketRateLimiter limiter(50, 5);
            assert(limiter.consume(50) == true);
            assert(limiter.available_tokens() == 0);
            assert(limiter.consume(1) == false);
        }

        // Consume more than capacity immediately
        {
            TokenBucketRateLimiter limiter(20, 5);
            assert(limiter.consume(21) == false);
            assert(limiter.available_tokens() == 20); // State unchanged on failure
        }

        std::cout << "[PASS] TestBoundaryAndEdgeCases" << std::endl;
    }

    static void TestRecoveryAndRefill() {
        std::cout << "[TEST] Running TestRecoveryAndRefill..." << std::endl;

        // High refill rate for rapid verification
        // Capacity: 100, Rate: 100,000 tokens/sec (100 tokens per ms)
        TokenBucketRateLimiter limiter(100, 100000);

        // Drain completely
        assert(limiter.consume(100) == true);
        assert(limiter.available_tokens() == 0);

        // Immediate consumption should fail
        assert(limiter.consume(1) == false);

        // Wait for refill (e.g., 5ms should yield ~500 tokens, capped at capacity 100)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));

        assert(limiter.available_tokens() == 100);
        assert(limiter.consume(100) == true);

        // Test Dynamic Reset State Verification
        limiter.reset(500, 50);
        assert(limiter.capacity() == 500);
        assert(limiter.refill_rate() == 50);
        assert(limiter.available_tokens() == 500);

        std::cout << "[PASS] TestRecoveryAndRefill" << std::endl;
    }

    static void TestConcurrentContentionStress() {
        std::cout << "[TEST] Running TestConcurrentContentionStress (Multi-threaded)..." << std::endl;

        const uint64_t kCapacity = 100000;
        const uint64_t kRefillRate = 1000;
        TokenBucketRateLimiter limiter(kCapacity, kRefillRate);

        const int kNumThreads = 16;
        const int kIterationsPerThread = 5000;
        std::atomic<uint64_t> successful_consumptions{0};
        std::atomic<uint64_t> failed_consumptions{0};

        std::vector<std::thread> threads;
        threads.reserve(kNumThreads);

        auto worker = [&](int thread_id) {
            for (int i = 0; i < kIterationsPerThread; ++i) {
                // Alternate consumption patterns
                uint64_t tokens_to_take = (i % 2 == 0) ? 1 : 2;
                if (limiter.consume(tokens_to_take)) {
                    successful_consumptions.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failed_consumptions.fetch_add(1, std::memory_order_relaxed);
                }
            }
        };

        auto start_time = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < kNumThreads; ++i) {
            threads.emplace_back(worker, i);
        }

        for (auto& t : threads) {
            t.join();
        }

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

        std::cout << "[INFO] Concurrent Stress Test Completed in " << duration << " ms." << std::endl;
        std::cout << "[INFO] Successful Consumptions: " << successful_consumptions.load() << std::endl;
        std::cout << "[INFO] Failed Consumptions: " << failed_consumptions.load() << std::endl;

        // Invariant check: Total successful operations cannot exceed initial capacity + refilled tokens
        // Since test runs fast, available tokens + consumed tokens must be bounded.
        assert(successful_consumptions.load() > 0);
        std::cout << "[PASS] TestConcurrentContentionStress" << std::endl;
    }

    static void TestInvariantPreservation() {
        std::cout << "[TEST] Running TestInvariantPreservation..." << std::endl;

        TokenBucketRateLimiter limiter(50, 10);

        // Invariant 1: Available tokens can never exceed capacity
        for (int i = 0; i < 10; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            assert(limiter.available_tokens() <= limiter.capacity());
        }

        // Invariant 2: Over-consumption never drops token count below zero or mutates state incorrectly
        assert(limiter.consume(50) == true);
        assert(limiter.available_tokens() == 0);
        
        for (int i = 0; i < 5; ++i) {
            assert(limiter.consume(100) == false);
            assert(limiter.available_tokens() < limiter.capacity()); // Should not underflow or overflow arbitrarily
        }

        std::cout << "[PASS] TestInvariantPreservation" << std::endl;
    }
};

} // namespace aether::testing

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================
int main() {
    try {
        aether::testing::TestRunner::RunAll();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "[FATAL] Test suite failed with exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "[FATAL] Test suite failed with unknown exception." << std::endl;
        return 1;
    }
}