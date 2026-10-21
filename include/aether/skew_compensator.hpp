#ifndef AETHER_SKEW_COMPENSATOR_HPP
#define AETHER_SKEW_COMPENSATOR_HPP

#include <atomic>
#include <cstdint>
#include <chrono>
#include <array>
#include <algorithm>
#include <system_error>
#include <new>

namespace aether {

// System-wide constants based on specifications
constexpr std::size_t MAX_CLUSTER_PEERS = 64;
constexpr int64_t MAX_CLOCK_DRIFT_NS = 100'000'000; // ±100ms in nanoseconds
constexpr std::size_t RING_BUFFER_CAPACITY = 1024;   // Power of two

enum class SchedError {
    Success = 0,
    ClockDriftExceeded,
    PartitionDetected,
    PeerNotFound,
    BufferFull
};

class SchedErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "aether_skew_compensator"; }
    std::string message(int ev) const override {
        switch (static_cast<SchedError>(ev)) {
            case SchedError::Success: return "Success";
            case SchedError::ClockDriftExceeded: return "Clock drift anomaly detected (> ±100ms)";
            case SchedError::PartitionDetected: return "Network partition / split-brain fallback active";
            case SchedError::PeerNotFound: return "Peer node not found in routing topology";
            case SchedError::BufferFull: return "Telemetry ring buffer capacity exhausted (> 95%)";
            default: return "Unknown system error";
        }
    }
};

inline const std::error_category& sched_error_category() noexcept {
    static SchedErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(SchedError e) noexcept {
    return {static_cast<int>(e), sched_error_category()};
}

} // namespace aether

namespace std {
template <>
struct is_error_code_enum<aether::SchedError> : true_type {};
} // namespace std

namespace aether {

// 16-byte aligned hybrid atomic structure for token bucket & sliding window
struct alignas(16) HybridTokenBucket {
    uint64_t tokens;
    uint64_t timestamp_ns;

    bool compare_exchange(uint64_t& expected_tokens, uint64_t& expected_ts,
                          uint64_t desired_tokens, uint64_t desired_ts) noexcept {
        // Use double-word CAS via GCC/Clang __int128 extension for atomic 16-byte update
        using uint128_t = unsigned __int128;
        
        uint128_t expected_val = (static_cast<uint128_t>(expected_ts) << 64) | expected_tokens;
        uint128_t desired_val  = (static_cast<uint128_t>(desired_ts) << 64) | desired_tokens;
        
        // Aliasing the struct memory as an atomic 128-bit integer
        auto* atomic_ptr = reinterpret_cast<std::atomic<uint128_t>*>(this);
        
        bool success = atomic_ptr->compare_exchange_strong(
            expected_val, desired_val,
            std::memory_order_acq_rel,
            std::memory_order_acquire
        );

        if (!success) {
            expected_tokens = static_cast<uint64_t>(expected_val);
            expected_ts = static_cast<uint64_t>(expected_val >> 64);
        }
        return success;
    }
};

// Fixed-capacity, power-of-two lock-free circular queue with cache-line isolation
template <typename T, std::size_t Capacity>
class alignas(64) RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
public:
    RingBuffer() : head_(0), tail_(0) {
        for (std::size_t i = 0; i < Capacity; ++i) {
            slots_[i].state.store(SlotState::Empty, std::memory_order_relaxed);
        }
    }

    ~RingBuffer() = default;

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;

    bool push(const T& item) noexcept {
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        const uint64_t head = head_.load(std::memory_order_acquire);

        if ((tail - head) >= Capacity) {
            return false; // Full
        }

        auto& slot = slots_[tail & (Capacity - 1)];
        SlotState expected = SlotState::Empty;
        if (!slot.state.compare_exchange_strong(expected, SlotState::Writing, std::memory_order_acquire, std::memory_order_relaxed)) {
            return false;
        }

        slot.data = item;
        slot.state.store(SlotState::Ready, std::memory_order_release);
        tail_.store(tail + 1, std::memory_order_seq_cst);
        return true;
    }

    bool pop(T& item) noexcept {
        const uint64_t head = head_.load(std::memory_order_relaxed);
        const uint64_t tail = tail_.load(std::memory_order_acquire);

        if (head == tail) {
            return false; // Empty
        }

        auto& slot = slots_[head & (Capacity - 1)];
        SlotState expected = SlotState::Ready;
        if (!slot.state.compare_exchange_strong(expected, SlotState::Reading, std::memory_order_acquire, std::memory_order_relaxed)) {
            return false;
        }

        item = slot.data;
        slot.state.store(SlotState::Empty, std::memory_order_release);
        head_.store(head + 1, std::memory_order_seq_cst);
        return true;
    }

    double fill_ratio() const noexcept {
        const uint64_t head = head_.load(std::memory_order_relaxed);
        const uint64_t tail = tail_.load(std::memory_order_relaxed);
        uint64_t size = (tail >= head) ? (tail - head) : 0;
        return static_cast<double>(size) / static_cast<double>(Capacity);
    }

private:
    enum class SlotState : uint8_t { Empty, Writing, Ready, Reading };

    struct Slot {
        std::atomic<SlotState> state;
        T data;
    };

    alignas(64) std::atomic<uint64_t> head_;
    alignas(64) std::atomic<uint64_t> tail_;
    alignas(64) std::array<Slot, Capacity> slots_;
};

// Peer telemetry structure for Marzullo skew compensation
struct PeerSample {
    uint64_t lamport_timestamp;
    int64_t clock_offset_ns;
    int64_t rtt_ns;
    bool active;
};

// Decentralized Marzullo-variant Skew Compensator
class SkewCompensator {
public:
    SkewCompensator() 
        : local_lamport_(0), peer_count_(0) {
        for (std::size_t i = 0; i < MAX_CLUSTER_PEERS; ++i) {
            peers_[i].lamport_timestamp = 0;
            peers_[i].clock_offset_ns = 0;
            peers_[i].rtt_ns = 0;
            peers_[i].active = false;
        }
    }

    ~SkewCompensator() = default;

    void update_peer(std::size_t peer_id, uint64_t lamport_ts, int64_t offset_ns, int64_t rtt_ns) noexcept {
        if (peer_id >= MAX_CLUSTER_PEERS) {
            return;
        }

        // Isolate peer if clock drift exceeds ±100ms
        if (offset_ns > MAX_CLOCK_DRIFT_NS || offset_ns < -MAX_CLOCK_DRIFT_NS) {
            peers_[peer_id].active = false;
            return;
        }

        peers_[peer_id].lamport_timestamp = lamport_ts;
        peers_[peer_id].clock_offset_ns = offset_ns;
        peers_[peer_id].rtt_ns = rtt_ns;
        peers_[peer_id].active = true;

        // Update global Lamport causality baseline
        uint64_t current_lamport = local_lamport_.load(std::memory_order_relaxed);
        while (lamport_ts > current_lamport && 
               !local_lamport_.compare_exchange_weak(current_lamport, lamport_ts, std::memory_order_acq_rel, std::memory_order_relaxed)) {
            // Spin-loop for updating Lamport clock
        }
    }

    std::error_code compute_cluster_median_offset(int64_t& out_median_offset) noexcept {
        int64_t offsets[MAX_CLUSTER_PEERS];
        std::size_t active_count = 0;

        for (std::size_t i = 0; i < MAX_CLUSTER_PEERS; ++i) {
            if (peers_[i].active) {
                // Adjust bound by half RTT for Marzullo interval evaluation
                offsets[active_count++] = peers_[i].clock_offset_ns;
            }
        }

        if (active_count == 0) {
            out_median_offset = 0;
            return make_error_code(SchedError::PartitionDetected);
        }

        std::sort(offsets, offsets + active_count);

        if (active_count % 2 == 1) {
            out_median_offset = offsets[active_count / 2];
        } else {
            out_median_offset = (offsets[(active_count / 2) - 1] + offsets[active_count / 2]) / 2;
        }

        // Validate resulting median offset against bounds
        if (out_median_offset > MAX_CLOCK_DRIFT_NS || out_median_offset < -MAX_CLOCK_DRIFT_NS) {
            return make_error_code(SchedError::ClockDriftExceeded);
        }

        return make_error_code(SchedError::Success);
    }

    uint64_t increment_lamport() noexcept {
        return local_lamport_.fetch_add(1, std::memory_order_acq_rel) + 1;
    }

    uint64_t get_lamport() const noexcept {
        return local_lamport_.load(std::memory_order_acquire);
    }

private:
    std::atomic<uint64_t> local_lamport_;
    std::size_t peer_count_;
    PeerSample peers_[MAX_CLUSTER_PEERS];
};

} // namespace aether

#endif // AETHER_SKEW_COMPENSATOR_HPP