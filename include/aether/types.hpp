#ifndef AETHER_TYPES_HPP
#define AETHER_TYPES_HPP

#include <cstdint>
#include <atomic>
#include <array>
#include <new>
#include <chrono>
#include <system_error>
#include <type_traits>

namespace aether {

namespace detail {
    // Helper to detect 128-bit atomic support for lock-free 16-byte aligned operations
    template <typename T>
    inline constexpr bool is_lock_free_128() noexcept {
        #if defined(AETHER_HAS_INT128) || defined(__SIZEOF_INT128__)
        return sizeof(T) == 16 && alignof(T) == 16;
        #else
        return false;
        #endif
    }
}

// 16-byte aligned hybrid atomic struct for Token Bucket & Sliding Window state.
// Packs tokens (64-bit) and timestamp_ns (64-bit) into a 16-byte block to allow 
// single-instruction double-word atomic operations (__int128).
struct alignas(16) PackedBucketState {
    uint64_t tokens;
    uint64_t timestamp_ns;

    constexpr PackedBucketState() noexcept : tokens(0), timestamp_ns(0) {}
    constexpr PackedBucketState(uint64_t t, uint64_t ts) noexcept : tokens(t), timestamp_ns(ts) {}

    friend bool operator==(const PackedBucketState& lhs, const PackedBucketState& rhs) noexcept {
        return lhs.tokens == rhs.tokens && lhs.timestamp_ns == rhs.timestamp_ns;
    }

    friend bool operator!=(const PackedBucketState& lhs, const PackedBucketState& rhs) noexcept {
        return !(lhs == rhs);
    }
};

// Ensure standard layout and trivial copyability for atomic operations
static_assert(std::is_trivial_v<PackedBucketState>, "PackedBucketState must be trivial");
static_assert(sizeof(PackedBucketState) == 16, "PackedBucketState must be exactly 16 bytes");
static_assert(alignof(PackedBucketState) == 16, "PackedBucketState must be 16-byte aligned");

// Atomic wrapper for PackedBucketState using hardware 128-bit CAS if available,
// falling back or explicitly utilizing unsigned __int128 storage.
class AtomicBucketState {
private:
    #if defined(__SIZEOF_INT128__)
    using atomic_storage_t = unsigned __int128;
    
    static_assert(sizeof(atomic_storage_t) == 16, "128-bit integer size mismatch");

    std::atomic<atomic_storage_t> storage_;

    static constexpr atomic_storage_t pack(const PackedBucketState& state) noexcept {
        return (static_cast<atomic_storage_t>(state.timestamp_ns) << 64) | 
               (static_cast<atomic_storage_t>(state.tokens));
    }

    static constexpr PackedBucketState unpack(atomic_storage_t val) noexcept {
        return PackedBucketState{
            static_cast<uint64_t>(val & 0xFFFFFFFFFFFFFFFFULL),
            static_cast<uint64_t>(val >> 64)
        };
    }
    #else
    // Fallback alignment/storage if 128-bit integers are absent at compile time.
    // Note: System spec mandates 16-byte atomic capability; standard requires matching implementation.
    struct alignas(16) StorageStruct {
        uint64_t tokens;
        uint64_t timestamp_ns;
    };
    std::atomic<StorageStruct> storage_;
    #endif

public:
    constexpr AtomicBucketState() noexcept : storage_(0) {}
    
    explicit AtomicBucketState(const PackedBucketState& initial) noexcept {
        #if defined(__SIZEOF_INT128__)
        storage_.store(pack(initial), std::memory_order_relaxed);
        #else
        storage_.store({initial.tokens, initial.timestamp_ns}, std::memory_order_relaxed);
        #endif
    }

    AtomicBucketState(const AtomicBucketState&) = delete;
    AtomicBucketState& operator=(const AtomicBucketState&) = delete;
    AtomicBucketState(AtomicBucketState&&) = delete;
    AtomicBucketState& operator=(AtomicBucketState&&) = delete;

    ~AtomicBucketState() = default;

    bool compare_exchange_weak(PackedBucketState& expected, const PackedBucketState& desired,
                               std::memory_order success, std::memory_order failure) noexcept {
        #if defined(__SIZEOF_INT128__)
        atomic_storage_t exp = pack(expected);
        atomic_storage_t des = pack(desired);
        bool res = storage_.compare_exchange_weak(exp, des, success, failure);
        if (!res) {
            expected = unpack(exp);
        }
        return res;
        #else
        // Fallback implementation logic
        StorageStruct exp{expected.tokens, expected.timestamp_ns};
        StorageStruct des{desired.tokens, desired.timestamp_ns};
        bool res = storage_.compare_exchange_weak(exp, des, success, failure);
        if (!res) {
            expected = PackedBucketState{exp.tokens, exp.timestamp_ns};
        }
        return res;
        #endif
    }

    bool compare_exchange_strong(PackedBucketState& expected, const PackedBucketState& desired,
                                std::memory_order success, std::memory_order failure) noexcept {
        #if defined(__SIZEOF_INT128__)
        atomic_storage_t exp = pack(expected);
        atomic_storage_t des = pack(desired);
        bool res = storage_.compare_exchange_strong(exp, des, success, failure);
        if (!res) {
            expected = unpack(exp);
        }
        return res;
        #else
        StorageStruct exp{expected.tokens, expected.timestamp_ns};
        StorageStruct des{desired.tokens, desired.timestamp_ns};
        bool res = storage_.compare_exchange_strong(exp, des, success, failure);
        if (!res) {
            expected = PackedBucketState{exp.tokens, exp.timestamp_ns};
        }
        return res;
        #endif
    }

    PackedBucketState load(std::memory_order order) const noexcept {
        #if defined(__SIZEOF_INT128__)
        return unpack(storage_.load(order));
        #else
        StorageStruct s = storage_.load(order);
        return PackedBucketState{s.tokens, s.timestamp_ns};
        #endif
    }

    void store(const PackedBucketState& desired, std::memory_order order) noexcept {
        #if defined(__SIZEOF_INT128__)
        storage_.store(pack(desired), order);
        #else
        storage_.store({desired.tokens, desired.timestamp_ns}, order);
        #endif
    }
};

// Fixed-capacity, power-of-two lock-free circular queue utilizing sequential consistency.
// Cache-line padding isolates head and tail pointers to eliminate false sharing.
template <typename T, size_t Capacity>
class RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "RingBuffer Capacity must be a power of two");
    static_assert(Capacity > 0, "RingBuffer Capacity must be greater than zero");

private:
    struct Node {
        typename std::aligned_storage<sizeof(T>, alignof(T>>::type storage;
        std::atomic<size_t> sequence;

        Node() noexcept : sequence(0) {}
        
        ~Node() noexcept {
            // Destruction handled explicitly by the ring buffer container lifecycle
        }

        T* ptr() noexcept {
            return reinterpret_cast<T*>(&storage);
        }

        const T* ptr() const noexcept {
            return reinterpret_cast<const T*>(&storage);
        }
    };

    static constexpr size_t BUFFER_MASK = Capacity - 1;

    // Cache-line isolation for head pointer
    alignas(64) std::atomic<size_t> head_;
    
    // Cache-line isolation for tail pointer
    alignas(64) std::atomic<size_t> tail_;
    
    // Buffer storage container
    Node buffer_[Capacity];

public:
    RingBuffer() noexcept : head_(0), tail_(0) {
        for (size_t i = 0; i < Capacity; ++i) {
            buffer_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~RingBuffer() noexcept {
        T dummy;
        while (pop(dummy)) {
            // Drain remaining elements during destruction
        }
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    template <typename... Args>
    bool emplace(Args&&... args) noexcept {
        Node* node;
        size_t pos = head_.load(std::memory_order_seq_cst);
        
        for (;;) {
            node = &buffer_[pos & BUFFER_MASK];
            size_t seq = node->sequence.load(std::memory_order_seq_cst);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (dif == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1, 
                                                std::memory_order_seq_cst, 
                                                std::memory_order_seq_cst)) {
                    break;
                }
            } else if (dif < 0) {
                // Buffer is full
                return false;
            } else {
                pos = head_.load(std::memory_order_seq_cst);
            }
        }

        ::new (static_cast<void*>(node->ptr())) T(std::forward<Args>(args)...);
        node->sequence.store(pos + 1, std::memory_order_seq_cst);
        return true;
    }

    bool push(const T& value) noexcept {
        return emplace(value);
    }

    bool push(T&& value) noexcept {
        return emplace(std::move(value));
    }

    bool pop(T& value) noexcept {
        Node* node;
        size_t pos = tail_.load(std::memory_order_seq_cst);
        
        for (;;) {
            node = &buffer_[pos & BUFFER_MASK];
            size_t seq = node->sequence.load(std::memory_order_seq_cst);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (dif == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1, 
                                                std::memory_order_seq_cst, 
                                                std::memory_order_seq_cst)) {
                    break;
                }
            } else if (dif < 0) {
                // Buffer is empty
                return false;
            } else {
                pos = tail_.load(std::memory_order_seq_cst);
            }
        }

        value = std::move(*node->ptr());
        node->ptr()->~T();
        node->sequence.store(pos + Capacity, std::memory_order_seq_cst);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        size_t head = head_.load(std::memory_order_seq_cst);
        size_t tail = tail_.load(std::memory_order_seq_cst);
        return head == tail;
    }

    [[nodiscard]] size_t size() const noexcept {
        size_t head = head_.load(std::memory_order_seq_cst);
        size_t tail = tail_.load(std::memory_order_seq_cst);
        return (head >= tail) ? (head - tail) : (Capacity - (tail - head));
    }

    [[nodiscard]] constexpr size_t capacity() const noexcept {
        return Capacity;
    }
};

// Decentralized skew compensation and membership types
struct PeerClockSample {
    uint64_t node_id;
    int64_t lamport_timestamp;
    int64_t offset_ns;
    uint32_t rtt_ns;
    bool active;

    constexpr PeerClockSample() noexcept
        : node_id(0), lamport_timestamp(0), offset_ns(0), rtt_ns(0), active(false) {}
};

// Cluster topology & state descriptor for RCU propagation
struct ClusterTopologyVersion {
    uint64_t version_id;
    uint32_t active_peer_count;
    std::array<uint64_t, 64> active_node_ids;

    constexpr ClusterTopologyVersion() noexcept : version_id(0), active_peer_count(0), active_node_ids{} {}
};

// Error categorization for rate limiting and recovery subsystems
enum class AetherErrorCode : uint32_t {
    Success = 0,
    RateLimitExceeded,
    PartitionSplitBrainFallback,
    ClockDriftAnomalyDetected,
    RingBufferBackpressureDrop,
    ArenaAllocationExhaustion,
    InvalidTopologyState
};

class AetherErrorCategory : public std::error_category {
public:
    const char* name() const noexcept override {
        return "aether";
    }

    std::string message(int ev) const override {
        switch (static_cast<AetherErrorCode>(ev)) {
            case AetherErrorCode::Success:
                return "Operation completed successfully";
            case AetherErrorCode::RateLimitExceeded:
                return "Request rejected due to rate limit threshold constraints";
            case AetherErrorCode::PartitionSplitBrainFallback:
                return "Network partition detected; degrading to local node-budget isolation mode";
            case AetherErrorCode::ClockDriftAnomalyDetected:
                return "Monotonic clock deviation exceeds allowable safety bounds (> +/-100ms)";
            case AetherErrorCode::RingBufferBackpressureDrop:
                return "Ring buffer capacity >= 95%; dropping telemetry asynchronously";
            case AetherErrorCode::ArenaAllocationExhaustion:
                return "Custom arena allocator exhausted pre-allocated memory boundaries";
            case AetherErrorCode::InvalidTopologyState:
                return "Cluster topology descriptor version mismatch or invalid sequence";
            default:
                return "Unknown aether error system code";
        }
    }
};

inline const std::error_category& aether_category() noexcept {
    static AetherErrorCategory instance;
    return instance;
}

inline std::error_code make_error_code(AetherErrorCode e) noexcept {
    return {static_cast<int>(e), aether_category()};
}

} // namespace aether

namespace std {
    template <>
    struct is_error_code_enum<aether::AetherErrorCode> : true_type {};
}

#endif // AETHER_TYPES_HPP