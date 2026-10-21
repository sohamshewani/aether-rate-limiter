#ifndef AETHER_ALLOCATOR_HPP
#define AETHER_ALLOCATOR_HPP

#include <cstddef>
#include <cstdint>
#include <new>
#include <atomic>
#include <system_error>
#include <utility>

namespace aether {

enum class AllocatorError : uint8_t {
    Success = 0,
    OutOfMemory,
    AlignmentError,
    InvalidPointer
};

class AllocatorException : public std::system_error {
public:
    explicit AllocatorException(AllocatorError err, const char* msg)
        : std::system_error(static_cast<int>(err), std::generic_category(), msg),
          error_code_(err) {}

    [[nodiscard]] AllocatorError error() const noexcept { return error_code_; }

private:
    AllocatorError error_code_;
};

template <size_t TotalSize, size_t Alignment = 64>
class ArenaAllocator {
public:
    static_assert((Alignment & (Alignment - 1)) == 0, "Alignment must be a power of two");
    static_assert(TotalSize > 0, "Arena total size must be greater than zero");

    constexpr ArenaAllocator() noexcept
        : offset_(0) {
        // Ensure internal storage satisfies the requested alignment
        auto addr = reinterpret_cast<uintptr_t>(storage_);
        if ((addr % Alignment) != 0) {
            // If storage_ is not aligned by default, we adjust statically or handle dynamically.
            // alignas(Alignment) on storage_ guarantees correct alignment at compile-time.
        }
    }

    ~ArenaAllocator() = default;

    ArenaAllocator(const ArenaAllocator&) = delete;
    ArenaAllocator& operator=(const ArenaAllocator&) = delete;

    ArenaAllocator(ArenaAllocator&& other) noexcept
        : offset_(other.offset_.load(std::memory_order_relaxed)) {
        // Copy memory content
        for (size_t i = 0; i < TotalSize; ++i) {
            storage_[i] = other.storage_[i];
        }
    }

    ArenaAllocator& operator=(ArenaAllocator&& other) noexcept {
        if (this != &other) {
            offset_.store(other.offset_.load(std::memory_order_relaxed), std::memory_order_relaxed);
            for (size_t i = 0; i < TotalSize; ++i) {
                storage_[i] = other.storage_[i];
            }
        }
        return *this;
    }

    [[nodiscard]] void* allocate(size_t size, size_t alignment = Alignment) {
        if (size == 0) {
            return nullptr;
        }

        if ((alignment & (alignment - 1)) != 0) {
            throw AllocatorException(AllocatorError::AlignmentError, "Invalid alignment requested");
        }

        size_t current_offset = offset_.load(std::memory_order_relaxed);
        
        while (true) {
            uintptr_t current_address = reinterpret_cast<uintptr_t>(storage_) + current_offset;
            size_t adjustment = calculate_alignment_offset(current_address, alignment);
            size_t needed = size + adjustment;

            if (current_offset + needed > TotalSize) {
                throw AllocatorException(AllocatorError::OutOfMemory, "Arena allocator exhausted");
            }

            size_t next_offset = current_offset + needed;
            if (offset_.compare_exchange_weak(current_offset, next_offset,
                                              std::memory_order_release,
                                              std::memory_order_relaxed)) {
                uintptr_t aligned_address = current_address + adjustment;
                return reinterpret_cast<void*>(aligned_address);
            }
        }
    }

    void deallocate(void* ptr, size_t size) noexcept {
        // Arena allocators are designed for monotonic allocation where individual
        // deallocations are no-ops. Resetting the entire arena is done via reset().
        (void)ptr;
        (void)size;
    }

    void reset() noexcept {
        offset_.store(0, std::memory_order_release);
    }

    [[nodiscard]] size_t allocated_bytes() const noexcept {
        return offset_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] static constexpr size_t capacity() noexcept {
        return TotalSize;
    }

    template <typename T, typename... Args>
    [[nodiscard]] T* create(Args&&... args) {
        void* mem = allocate(sizeof(T), alignof(T));
        if (mem == nullptr) {
            throw AllocatorException(AllocatorError::OutOfMemory, "Failed to allocate memory for object");
        }
        return ::new (mem) T(std::forward<Args>(args)...);
    }

    template <typename T>
    void destroy(T* ptr) noexcept {
        if (ptr != nullptr) {
            ptr->~T();
        }
    }

private:
    [[nodiscard]] static constexpr size_t calculate_alignment_offset(uintptr_t address, size_t alignment) noexcept {
        size_t modulo = address & (alignment - 1);
        if (modulo == 0) {
            return 0;
        }
        return alignment - modulo;
    }

    alignas(Alignment) char storage_[TotalSize];
    std::atomic<size_t> offset_;
};

template <typename T, size_t TotalSize, size_t Alignment = 64>
class ArenaStlAdapter {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = ArenaStlAdapter<U, TotalSize, Alignment>;
    };

    explicit ArenaStlAdapter(ArenaAllocator<TotalSize, Alignment>& arena) noexcept
        : arena_(&arena) {}

    template <typename U>
    ArenaStlAdapter(const ArenaStlAdapter<U, TotalSize, Alignment>& other) noexcept
        : arena_(other.arena()) {}

    ~ArenaStlAdapter() = default;

    [[nodiscard]] T* allocate(std::size_t n) {
        if (n == 0) {
            return nullptr;
        }
        if (n > static_cast<std::size_t>(-1) / sizeof(T)) {
            throw AllocatorException(AllocatorError::OutOfMemory, "Integer overflow in STL adapter allocation");
        }
        void* ptr = arena_->allocate(n * sizeof(T), alignof(T));
        return static_cast<T*>(ptr);
    }

    void deallocate(T* p, std::size_t n) noexcept {
        if (arena_ != nullptr) {
            arena_->deallocate(p, n * sizeof(T));
        }
    }

    [[nodiscard]] ArenaAllocator<TotalSize, Alignment>* arena() const noexcept {
        return arena_;
    }

    template <typename U>
    [[nodiscard]] bool operator==(const ArenaStlAdapter<U, TotalSize, Alignment>& other) const noexcept {
        return arena_ == other.arena();
    }

    template <typename U>
    [[nodiscard]] bool operator!=(const ArenaStlAdapter<U, TotalSize, Alignment>& other) const noexcept {
        return arena_ != other.arena();
    }

private:
    ArenaAllocator<TotalSize, Alignment>* arena_;
};

} // namespace aether

#endif // AETHER_ALLOCATOR_HPP