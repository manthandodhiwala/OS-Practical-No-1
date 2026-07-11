# Name: Manthan Dodhiwala
# Roll No.: S083

print("Name: Manthan Dodhiwala")
print("Roll No.: S083")

from multiprocessing import Process, Semaphore, Lock, Array, Value
import time
import random

# Total buffer size
BUFFER_SIZE = 5

# Total number of items to produce and consume
ITEM_COUNT = 10

# Producer Function
def producer(buffer, in_index, out_index, empty, full, mutex):
    for _ in range(ITEM_COUNT):
        item = random.randint(1, 100)  # Produce a random number

        empty.acquire()  # Wait until there is an empty slot
        mutex.acquire()  # Enter critical section

        # Place the item in the buffer
        idx = in_index.value
        buffer[idx] = item
        print(f"[Producer] Produced item {item} at index {idx}", flush=True)

        # Update in_index
        in_index.value = (idx + 1) % BUFFER_SIZE

        mutex.release()  # Leave critical section
        full.release()   # Signal that an item is available

        # Simulate delay
        time.sleep(random.uniform(0.1, 0.3))


# Consumer Function
def consumer(buffer, in_index, out_index, empty, full, mutex):
    print("[Consumer] Process started", flush=True)

    for _ in range(ITEM_COUNT):
        full.acquire()   # Wait until there is an item
        mutex.acquire()  # Enter critical section

        # Remove item from the buffer
        idx = out_index.value
        item = buffer[idx]
        print(f"[Consumer] Consumed item {item} from index {idx}", flush=True)

        # Update out_index
        out_index.value = (idx + 1) % BUFFER_SIZE

        mutex.release()  # Leave critical section
        empty.release()  # Signal that a slot is free

        # Simulate delay
        time.sleep(random.uniform(0.1, 0.3))


def main():
    # Shared memory
    buffer = Array('i', BUFFER_SIZE)
    in_index = Value('i', 0)
    out_index = Value('i', 0)

    # Synchronization primitives
    empty = Semaphore(BUFFER_SIZE)
    full = Semaphore(0)
    mutex = Lock()

    print("Starting processes...", flush=True)

    # Create producer and consumer processes
    p = Process(target=producer, args=(buffer, in_index, out_index, empty, full, mutex))
    c = Process(target=consumer, args=(buffer, in_index, out_index, empty, full, mutex))

    # Start processes
    p.start()
    c.start()

    # Wait for processes to finish
    p.join()
    c.join()

    print("Producer and Consumer processes have finished.", flush=True)


if __name__ == "__main__":
    main()
