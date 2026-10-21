#ifndef AETHER_CORE_CLUSTER_NODE_HPP
#define AETHER_CORE_CLUSTER_NODE_HPP

#include <cstdint>
#include <atomic>
#include <chrono>
#include <array>
#include <vector>
#include <string>
#include <system_error>
#include <memory>
#include <new>
#include <algorithm>
#include <cmath>
#include <cstring>

namespace aether::core {

// System Constants
constexpr size_t CACHE_LINE_SIZE = 64;
constexpr uint64_t MAX_PEERS = 64;
constexpr int64_t MAX_CLOCK_DRIFT_NS = 100'000'000; // 100ms
constexpr int64_t SYNCHRONIZATION_WINDOW_NS = 50'000'000; // 50ms
constexpr double RING_BUFFER_BACKPRESSURE_THRESHOLD = 0.95;

// Error Hierarchy
enum class ClusterNodeError {
    Success = 0,
    RingBufferFull,
    RingBufferEmpty,
    ClockDriftAnomalyDetected,
    SplitBrainPartition,
    TokenLimitExceeded,
    InvalidConfiguration,
    PeerNotFound
};

class ClusterNodeCategory : public std::error_category {
public:
    const char* name() const noexcept override { return "ClusterNode"; }
    std::string message(int ev) const override {
        switch (static_cast<ClusterNodeError>(ev)) {
            case ClusterNodeError::Success: return "Success";
            case ClusterNodeError::RingBufferFull: return "Ring buffer is full (Backpressure)";
            case ClusterNodeError::RingBufferEmpty: return "Ring buffer is empty";
            case ClusterNodeError::ClockDriftAnomalyDetected: return "Peer clock drift exceeds threshold";
            case ClusterNodeError::SplitBrainPartition: return "Network partition detected, operating in local isolation";
            case ClusterNodeError::TokenLimitExceeded: return "Rate limit token consumption denied";
            case ClusterNodeError::InvalidConfiguration: return "Invalid structural configuration";
            case ClusterNodeError::PeerNotFound: return "Target peer node not found in registry";
            default: return "Unknown cluster node error";
        }
    }
};

inline const std::error_category& get_cluster_node_category() noexcept {
    static ClusterNodeCategory instance;
    return instance;
}

inline std::error_code make_error_code(ClusterNodeError e) noexcept {
    return {static_cast<int>(e), get_cluster_node_category()};
}

} // namespace aether::core

namespace std {
template <>
struct is_error_code_enum<aether::core::ClusterNodeError> : true_type {};
}

namespace aether::core {

// 1. DATA STRUCTURES & MEMORY LAYOUT

// Hybrid atomic struct for token bucket & sliding window
struct alignas(16) HybridTokenBucket {
    union State {
        __int128 raw;
        struct {
            uint64_t tokens;
            uint64_t timestamp_ns;
        } fields;
    };

    std::atomic<__int128> data;

    HybridTokenBucket(uint64_t initial_tokens, uint64_t initial_ts) noexcept {
        State s;
        s.fields.tokens = initial_tokens;
        s.fields.timestamp_ns = initial_ts;
        data.store(s.raw, std::memory_order_relaxed);
    }

    bool compare_exchange(uint64_t& expected_tokens, uint64_t& expected_ts,
                          uint64_t desired_tokens, uint64_t desired_ts) noexcept {
        State expected;
        expected.fields.tokens = expected_tokens;
        expected.fields.timestamp_ns = expected_ts;

        State desired;
        desired.fields.tokens = desired_tokens;
        desired.fields.timestamp_ns = desired_ts;

        return data.compare_exchange_weak(expected.raw, desired.raw,
                                          std::memory_order_seq_cst,
                                          std::memory_order_acquire);
    }

    void load(uint64_t& out_tokens, uint64_t& out_ts) const noexcept {
        State s;
        s.raw = data.load(std::memory_order_acquire);
        out_tokens = s.fields.tokens;
        out_ts = s.fields.timestamp_ns;
    }
};

// Fixed-capacity, power-of-two lock-free circular queue
template <typename T, size_t Capacity>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two.");
public:
    RingBuffer() : head_(0), tail_(0) {
        std::memset(buffer_, 0, sizeof(buffer_));
    }

    ~RingBuffer() {
        T dummy;
        while (pop(dummy) == ClusterNodeError::Success);
    }

    ClusterNodeError push(const T& item) noexcept {
        size_t current_tail = tail_.load(std::memory_order_seq_cst);
        size_t current_head = head_.load(std::memory_order_seq_cst);

        if ((current_tail - current_head) >= Capacity) {
            return ClusterNodeError::RingBufferFull;
        }

        if (tail_.compare_exchange_strong(current_tail, current_tail + 1,
                                          std::memory_order_seq_cst,
                                          std::memory_order_seq_cst)) {
            buffer_[current_tail & Mask] = item;
            return ClusterNodeError::Success;
        }
        return ClusterNodeError::RingBufferFull;
    }

    ClusterNodeError pop(T& item) noexcept {
        size_t current_head = head_.load(std::memory_order_seq_cst);
        size_t current_tail = tail_.load(std::memory_order_seq_cst);

        if (current_head == current_tail) {
            return ClusterNodeError::RingBufferEmpty;
        }

        if (head_.compare_exchange_strong(current_head, current_head + 1,
                                          std::memory_order_seq_cst,
                                          std::memory_order_seq_cst)) {
            item = buffer_[current_head & Mask];
            return ClusterNodeError::Success;
        }
        return ClusterNodeError::RingBufferEmpty;
    }

    double occupancy_ratio() const noexcept {
        size_t current_head = head_.load(std::memory_order_relaxed);
        size_t current_tail = tail_.load(std::memory_order_relaxed);
        size_t size = (current_tail >= current_head) ? (current_tail - current_head) : 0;
        return static_cast<double>(size) / static_cast<double>(Capacity);
    }

private:
    static constexpr size_t Mask = Capacity - 1;
    
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> head_;
    alignas(CACHE_LINE_SIZE) std::atomic<size_t> tail_;
    alignas(CACHE_LINE_SIZE) T buffer_[Capacity];
};

// Telemetry event for ring buffer ingestion
struct TelemetryEvent {
    uint64_t timestamp_ns;
    uint32_t node_id;
    uint32_t metric_type;
    int64_t value;
};

// Skew Compensation: Decentralized Marzullo-variant algorithm
struct PeerHeartbeat {
    uint32_t node_id;
    uint64_t lamport_timestamp;
    int64_t clock_offset_ns;
    int64_t rtt_ns;
    bool active;
};

class SkewCompensator {
public:
    SkewCompensator() : peer_count_(0) {
        std::memset(peers_, 0, sizeof(peers_));
    }

    void update_peer(uint32_t node_id, uint64_t lamport_ts, int64_t offset_ns, int64_t rtt_ns) noexcept {
        for (size_t i = 0; i < MAX_PEERS; ++i) {
            if (peers_[i].node_id == node_id || !peers_[i].active) {
                peers_[i].node_id = node_id;
                peers_[i].lamport_timestamp = lamport_ts;
                peers_[i].clock_offset_ns = offset_ns;
                peers_[i].rtt_ns = rtt_ns;
                peers_[i].active = true;
                if (i >= peer_count_) {
                    peer_count_ = i + 1;
                }
                return;
            }
        }
    }

    int64_t compute_robust_median_offset() const noexcept {
        int64_t offsets[MAX_PEERS];
        size_t count = 0;

        for (size_t i = 0; i < peer_count_; ++i) {
            if (peers_[i].active) {
                if (std::abs(peers_[i].clock_offset_ns) <= MAX_CLOCK_DRIFT_NS) {
                    offsets[count++] = peers_[i].clock_offset_ns;
                }
            }
        }

        if (count == 0) return 0;

        std::sort(offsets, offsets + count);
        if (count % 2 == 0) {
            return (offsets[count / 2 - 1] + offsets[count / 2]) / 2;
        } else {
            return offsets[count / 2];
        }
    }

    bool validate_peer_drift(uint32_t node_id) const noexcept {
        for (size_t i = 0; i < peer_count_; ++i) {
            if (peers_[i].node_id == node_id && peers_[i].active) {
                return std::abs(peers_[i].clock_offset_ns) <= MAX_CLOCK_DRIFT_NS;
            }
        }
        return true; // Unknown peers default to valid until proven otherwise
    }

private:
    PeerHeartbeat peers_[MAX_PEERS];
    size_t peer_count_;
};

// 3. INVARIANTS & ARENA ALLOCATOR
class BootstrapArena {
public:
    explicit BootstrapArena(size_t size_bytes) 
        : size_(size_bytes), offset_(0) {
        memory_block_ = static_cast<std::byte*>(::operator new(size_bytes, std::align_val_t{CACHE_LINE_SIZE}));
        std::memset(memory_block_, 0, size_bytes);
    }

    ~BootstrapArena() {
        if (memory_block_) {
            ::operator delete(memory_block_, std::align_val_t{CACHE_LINE_SIZE});
        }
    }

    template <typename T, typename... Args>
    T* allocate(Args&&... args) {
        size_t space = size_ - offset_;
        void* ptr = memory_block_ + offset_;
        if (std::align(alignof(T), sizeof(T), ptr, space) == nullptr) {
            throw std::bad_alloc();
        }
        size_t aligned_offset = static_cast<size_t>(static_cast<std::byte*>(ptr) - memory_block_);
        offset_ = aligned_offset + sizeof(T);
        return new (ptr) T(std::forward<Args>(args)...);
    }

    void reset() noexcept {
        offset_ = 0;
    }

private:
    size_t size_;
    size_t offset_;
    std::byte* memory_block_;
};

// 2. CONCURRENCY & CLUSTER NODE CORE
class ClusterNode {
public:
    ClusterNode(uint32_t node_id, uint64_t max_capacity, uint64_t refill_rate_per_ns)
        : node_id_(node_id),
          max_capacity_(max_capacity),
          refill_rate_per_ns_(refill_rate_per_ns),
          bucket_(max_capacity, current_steady_clock_ns()),
          running_(false),
          partitioned_(false),
          lamport_clock_(0) {}

    ~ClusterNode() {
        stop();
    }

    void start() {
        running_.store(true, std::memory_order_seq_cst);
    }

    void stop() {
        running_.store(false, std::memory_order_seq_cst);
        worker_wake();
    }

    // Zero-lock execution path for token consumption
    std::error_code consume_tokens(uint64_t requested_tokens) noexcept {
        if (partitioned_.load(std::memory_order_acquire)) {
            return make_error_code(ClusterNodeError::SplitBrainPartition);
        }

        uint64_t current_tokens = 0;
        uint64_t last_ts = 0;
        uint64_t now = current_steady_clock_ns();

        while (true) {
            bucket_.load(current_tokens, last_ts);

            uint64_t time_delta = (now > last_ts) ? (now - last_ts) : 0;
            uint64_t generated = static_cast<uint64_t>(time_delta * refill_rate_per_ns_);
            
            // Invariant: Token Conservation - Tokens can never exceed capacity
            uint64_t potential_tokens = std::min(max_capacity_, current_tokens + generated);

            if (potential_tokens < requested_tokens) {
                return make_error_code(ClusterNodeError::TokenLimitExceeded);
            }

            uint64_t remaining_tokens = potential_tokens - requested_tokens;
            uint64_t new_ts = (potential_tokens == max_capacity_ && current_tokens == max_capacity_) ? last_ts : now;

            if (bucket_.compare_exchange(current_tokens, last_ts, remaining_tokens, new_ts)) {
                return make_error_code(ClusterNodeError::Success);
            }
        }
    }

    // OOM / Backpressure handling for ring buffer ingestion
    std::error_code ingest_telemetry(const TelemetryEvent& event) noexcept {
        if (telemetry_ring_.occupancy_ratio() >= RING_BUFFER_BACKPRESSURE_THRESHOLD) {
            // Shed telemetry load asynchronously, return success for shedding but drop event
            return make_error_code(ClusterNodeError::RingBufferFull);
        }
        return telemetry_ring_.push(event);
    }

    void update_topology(uint32_t peer_id, uint64_t lamport_ts, int64_t offset_ns, int64_t rtt_ns) noexcept {
        lamport_clock_.store(std::max(lamport_clock_.load(std::memory_order_relaxed), lamport_ts) + 1, std::memory_order_relaxed);
        skew_compensator_.update_peer(peer_id, lamport_ts, offset_ns, rtt_ns);

        // Check clock drift anomaly isolation
        if (!skew_compensator_.validate_peer_drift(peer_id)) {
            // Isolate peer by forcing partition mode evaluation
            partitioned_.store(true, std::memory_order_release);
        }
    }

    void evaluate_cluster_health() noexcept {
        int64_t median_offset = skew_compensator_.compute_robust_median_offset();
        if (std::abs(median_offset) > MAX_CLOCK_DRIFT_NS) {
            partitioned_.store(true, std::memory_order_release);
        } else {
            partitioned_.store(false, std::memory_order_release);
        }
    }

    // Futex-backed wait/wake primitive integration
    void worker_wait() noexcept {
        uint64_t expected = running_.load(std::memory_order_relaxed);
        if (expected) {
            running_.wait(true, std::memory_order_acquire);
        }
    }

    void worker_wake() noexcept {
        running_.notify_one();
    }

    bool is_running() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

    uint32_t get_node_id() const noexcept {
        return node_id_;
    }

private:
    uint64_t current_steady_clock_ns