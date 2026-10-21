#pragma once

#include <atomic>
#include <cstdint>
#include <chrono>
#include <array>
#include <new>
#include <system_error>
#include <optional>

namespace aether::core {

namespace detail {
    inline uint64_t monotonic_time_ns() noexcept {
        struct timespec ts;
        // CLOCK_MONOTONIC ensures stability against system clock jumps
        clock_gettime(CLOCK_MONOTONIC, &ts);
        return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + static_cast<uint64_t>(ts.tv_nsec);
    }
}

// 16-byte aligned packed atomic state for single-instruction double-word CAS updates.
// Combines tokens and timestamp into a 128-bit value.
struct alignas(16) PackedBucketState {
    uint64_t tokens;
    uint64_t timestamp_ns;

    bool operator==(const PackedBucketState& other) const noexcept {
        return tokens == other.tokens && timestamp_ns == other.timestamp_ns;
    }
};

static_assert(sizeof(PackedBucketState) == 16, "PackedBucketState must be exactly 16 bytes for double-word atomic operations.");

class AtomicBucketState {
private:
    alignas(16) std::atomic<__int128> state_;

    __int128 pack(const PackedBucketState& b) const noexcept {
        return (static_cast<__int128>(b.timestamp_ns) << 64) | (b.tokens & 0xFFFFFFFFFFFFFFFFULL);
    }

    PackedBucketState unpack(__int128 raw) const noexcept {
        PackedBucketState b;
        b.tokens = static_cast<uint64_t>(raw & 0xFFFFFFFFFFFFFFFFULL);
        b.timestamp_ns = static_cast<uint64_t>(raw >> 64);
        return b;
    }

public:
    explicit AtomicBucketState(uint64_t initial_tokens, uint64_t initial_ts) noexcept {
        PackedBucketState initial{initial_tokens, initial_ts};
        state_.store(pack(initial), std::memory_order_relaxed);
    }

    PackedBucketState load(std::memory_order order = std::memory_order_acquire) const noexcept {
        __int128 raw = state_.load(order);
        return unpack(raw);
    }

    bool compare_exchange_weak(PackedBucketState& expected, const PackedBucketState& desired,
                               std::memory_order success, std::memory_order failure) noexcept {
        __int128 exp_raw = pack(expected);
        __int128 des_raw = pack(desired);
        if (state_.compare_exchange_weak(exp_raw, des_raw, success, failure)) {
            return true;
        }
        expected = unpack(exp_raw);
        return false;
    }

    bool compare_exchange_strong(PackedBucketState& expected, const PackedBucketState& desired,
                                std::memory_order success, std::memory_order failure) noexcept {
        __int128 exp_raw = pack(expected);
        __int128 des_raw = pack(desired);
        if (state_.compare_exchange_strong(exp_raw, des_raw, success, failure)) {
            return true;
        }
        expected = unpack(exp_raw);
        return false;
    }
};

// Fixed-capacity, power-of-two lock-free circular queue with false-sharing isolation.
template <typename T, size_t Capacity>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "RingBuffer Capacity must be a power of two.");
private:
    struct Node {
        std::atomic<uint64_t> sequence{0};
        T data;
    };

    static constexpr size_t BUFFER_MASK = Capacity - 1;

    alignas(64) std::atomic<uint64_t> head_{0};
    alignas(64) std::atomic<uint64_t> tail_{0};
    
    alignas(64) Node buffer_[Capacity];

public:
    RingBuffer() noexcept {
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~RingBuffer() = default;

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    bool push(const T& item) noexcept {
        Node* node;
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        for (;;) {
            node = &buffer_[tail & BUFFER_MASK];
            uint64_t seq = node->sequence.load(std::memory_order_acquire);
            int64_t dif = static_cast<int64_t>(seq) - static_cast<int64_t>(tail);
            if (dif == 0) {
                if (tail_.compare_exchange_weak(tail, tail + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Full
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }
        node->data = item;
        node->sequence.store(tail + 1, std::memory_order_seq_cst);
        return true;
    }

    bool pop(T& item) noexcept {
        Node* node;
        uint64_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            node = &buffer_[head & BUFFER_MASK];
            uint64_t seq = node->sequence.load(std::memory_order_acquire);
            int64_t dif = static_cast<int64_t>(seq) - static_cast<int64_t>(head + 1);
            if (dif == 0) {
                if (head_.compare_exchange_weak(head, head + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Empty
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }
        item = std::move(node->data);
        node->sequence.store(head + Capacity, std::memory_order_seq_cst);
        return true;
    }

    size_t size() const noexcept {
        uint64_t head = head_.load(std::memory_order_relaxed);
        uint64_t tail = tail_.load(std::memory_order_relaxed);
        if (tail >= head) {
            return static_cast<size_t>(tail - head);
        }
        return 0;
    }

    constexpr size_t capacity() const noexcept {
        return Capacity;
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_relaxed) == tail_.load(std::memory_order_relaxed);
    }
};

struct RateLimitConfig {
    uint64_t capacity;
    uint64_t refill_rate_per_sec;
};

class RateLimiterEngine {
private:
    RateLimitConfig config_;
    AtomicBucketState bucket_;
    std::atomic<bool> degraded_mode_{false};
    std::atomic<uint64_t> last_maintenance_ns_;

public:
    explicit RateLimiterEngine(const RateLimitConfig& config) noexcept
        : config_(config),
          bucket_(config.capacity, detail::monotonic_time_ns()),
          last_maintenance_ns_(detail::monotonic_time_ns()) {}

    ~RateLimiterEngine() = default;

    RateLimiterEngine(const RateLimiterEngine&) = delete;
    RateLimiterEngine& operator=(const RateLimiterEngine&) = delete;
    RateLimiterEngine(RateLimiterEngine&&) = delete;
    RateLimiterEngine& operator=(RateLimiterEngine&&) = delete;

    bool consume(uint64_t tokens_requested) noexcept {
        if (tokens_requested > config_.capacity) {
            return false;
        }

        uint64_t now = detail::monotonic_time_ns();
        PackedBucketState current = bucket_.load(std::memory_order_acquire);

        for (;;) {
            uint64_t elapsed_ns = now > current.timestamp_ns ? (now - current.timestamp_ns) : 0;
            uint64_t generated = (elapsed_ns * config_.refill_rate_per_sec) / 1000000000ULL;
            
            uint64_t new_tokens = current.tokens + generated;
            if (new_tokens > config_.capacity) {
                new_tokens = config_.capacity;
            }

            if (new_tokens < tokens_requested) {
                return false; // Rate limit exceeded
            }

            PackedBucketState desired{new_tokens - tokens_requested, now};
            if (bucket_.compare_exchange_weak(current, desired, std::memory_order_release, std::memory_order_acquire)) {
                return true;
            }
        }
    }

    void set_degraded_mode(bool degraded) noexcept {
        degraded_mode_.store(degraded, std::memory_order_release);
    }

    bool is_degraded() const noexcept {
        return degraded_mode_.load(std::memory_order_acquire);
    }

    void maintain() noexcept {
        uint64_t now = detail::monotonic_time_ns();
        last_maintenance_ns_.store(now, std::memory_order_relaxed);
        // Enforce invariants: Token conservation check via load & bounded window enforcement
        PackedBucketState current = bucket_.load(std::memory_order_relaxed);
        if (current.tokens > config_.capacity) {
            PackedBucketState corrected{config_.capacity, current.timestamp_ns};
            bucket_.compare_exchange_strong(current, corrected, std::memory_order_release, std::memory_order_relaxed);
        }
    }
};

} // namespace aether::core