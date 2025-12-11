

# PersistentDiskBackedMessageBroker

A lightweight, high-performance disk-backed message queue for Java.
Provides a durable FIFO queue using an append-only file, enabling reliable message storage across restarts.

---

## Features

* Persistent, append-only disk-backed queue
* Thread-safe enqueue() and dequeue()
* Zero-copy I/O using FileChannel and ByteBuffer
* Crash-safe message format
* Benchmarking tool included
* No external dependencies

---

## Project Structure

```
PersistentDiskBackedMessageBroker/
│
├── pom.xml
├── README.md
│
└── src/
    └── main/
        └── java/
            └── com/
                └── yourpackage/
                    ├── DiskQueue.java
                    ├── Main.java
                    └── Benchmark.java
```

---

## How It Works

Each message is stored as:

```
[4-byte message length][message bytes]
```

* enqueue() appends to the end of the file
* dequeue() reads the next record sequentially
* No full-file loading into memory
* Partial messages are safely ignored

---

## Usage Example

```java
DiskQueue queue = new DiskQueue("queue.dat");

queue.enqueue("hello".getBytes());

byte[] msg = queue.dequeue();
System.out.println(new String(msg));
```

---

## Benchmark

To run the benchmark:

```
java Benchmark
```

Measures:

* Enqueue throughput
* Dequeue throughput
* Total execution time

---

## Thread Safety

enqueue() and dequeue() are synchronized for basic thread safety.

Planned improvements:

* Read/write locks
* Lock-free writer
* Batch operations

---

## Requirements

* Java 11 or higher
* Works on Windows, Linux, and macOS

---

## Running the Project

Compile:

```
javac *.java
```

Run:

```
java Main
```

Delete queue file:

```
rm queue.dat   (Mac/Linux)
del queue.dat  (Windows)
```

---

## Future Improvements

* File compaction
* Memory-mapped I/O
* Metadata file for read/write pointers
* Multi-topic queues
* Async producer/consumer APIs
* Pluggable serializers

---



