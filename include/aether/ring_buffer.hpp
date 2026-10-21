#ifndef AETHER_RING_BUFFER_HPP
#define AETHER_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <optional>
#include <new>
#include <type_traits>

namespace aether {

template <typename T, std::size_t Capacity>
class alignas(64) RingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "RingBuffer Capacity must be a power of two.");
    static_assert(std::is_nothrow_destructible_v<T>, "RingBuffer requires nothrow destructible types.");
    static_assert(std::is_nothrow_constructible_v<T>, "RingBuffer requires nothrow constructible types.");

public:
    RingBuffer() noexcept 
        : head_(0), tail_(0) {
        for (std::size_t i = 0; i < Capacity; ++i) {
            sequence_[i].store(i, std::memory_order_relaxed);
        }
    }

    ~RingBuffer() noexcept {
        T dummy;
        while (pop(dummy)) {
            // Drain remaining elements to invoke destructors correctly.
        }
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    template <typename... Args>
    bool emplace(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) {
        std::size_t tail = tail_.load(std::memory_order_relaxed);
        
        for (;;) {
            std::atomic<std::size_t>& seq_ref = sequence_[tail & kMask];
            std::size_t seq = seq_ref.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(tail);

            if (dif == 0) {
                if (tail_.compare_exchange_weak(tail, tail + 1, 
                                                std::memory_order_seq_cst, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false;
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }

        ::new (static_cast<void*>(&buffer_[tail & kMask].storage)) T(std::forward<Args>(args)...);
        sequence_[tail & kMask].store(tail + 1, std::memory_order_release);
        return true;
    }

    bool push(const T& item) noexcept(std::is_nothrow_copy_constructible_v<T>) {
        return emplace(item);
    }

    bool push(T&& item) noexcept(std::is_nothrow_move_constructible_v<T>) {
        return emplace(std::move(item));
    }

    bool pop(T& item) noexcept {
        std::size_t head = head_.load(std::memory_order_relaxed);

        for (;;) {
            std::atomic<std::size_t>& seq_ref = sequence_[head & kMask];
            std::size_t seq = seq_ref.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(head + 1);

            if (dif == 0) {
                if (head_.compare_exchange_weak(head, head + 1, 
                                                std::memory_order_seq_cst, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false;
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }

        T* ptr = reinterpret_cast<T*>(&buffer_[head & kMask].storage);
        item = std::move(*ptr);
        ptr->~T();

        sequence_[head & kMask].store(head + kMask + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] std::size_t size() const noexcept {
        std::size_t head = head_.load(std::memory_order_acquire);
        std::size_t tail = tail_.load(std::memory_order_acquire);
        if (tail >= head) {
            return tail - head;
        }
        return 0;
    }

    [[nodiscard]] bool empty() const noexcept {
        std::size_t head = head_.load(std::memory_order_acquire);
        std::size_t tail = tail_.load(std::memory_order_acquire);
        return tail == head;
    }

    [[nodiscard]] constexpr std::size_t capacity() const noexcept {
        return Capacity;
    }

    [[nodiscard]] double utilization() const noexcept {
        return static_cast<double>(size()) / static_cast<double>(Capacity);
    }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    struct Node {
        alignas(alignof(T)) char storage[sizeof(T)];
    };

    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;
    
    Node buffer_[Capacity];
    std::atomic<std::size_t> sequence_[Capacity];
};

} // namespace aether

#endif // AETHER_RING_BUFFER_HPP