package com.interview.timeseries;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class TimeSeriesStoreImpl implements TimeSeriesStore {

    //storage
    private final Map<String, NavigableMap<Long, List<DataPoint>>> metricStore = new ConcurrentHashMap<>();
    private final Map<String, ReentrantReadWriteLock> lockMap = new ConcurrentHashMap<>();
    private final Map<String, String> stringCache = new ConcurrentHashMap<>();

    //constants
    private final String DATA_FILE_PATH;
    private final long retentionPeriodMs;
    private static final int BATCH_SIZE = 100;
    private static final int FLUSH_INTERVAL_MS = 1000;
    private static final long CLEANUP_INTERVAL_MS = 30*60 * 1000;

    ReentrantLock fileLock = new ReentrantLock(true);
    private boolean initialized = false;
    private volatile boolean cleanupRunning = false;
    private volatile boolean writerRunning = false;

    private final BlockingQueue<DataPoint> writeQueue = new LinkedBlockingQueue<>();
    private Thread cleanupThread;
    private Thread writerThread;
    private BufferedWriter writer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ObjectWriter objectWriter = objectMapper.writer();

    private final ScheduledExecutorService fileCleanupExecutor = Executors.newSingleThreadScheduledExecutor();


    public TimeSeriesStoreImpl() {
        this("timeseries_data.jsonl",TimeUnit.HOURS.toMillis(24));
    }

    public TimeSeriesStoreImpl(String dataFilePath) {
        this(dataFilePath, TimeUnit.HOURS.toMillis(24));
    }

    public TimeSeriesStoreImpl(String dataFilePath,long retentionPeriodMs) {
        this.DATA_FILE_PATH = dataFilePath;
        this.retentionPeriodMs = retentionPeriodMs;
    }

    @Override
    public boolean insert(long timestamp, String metric, double value, Map<String, String> tags) {
        if (!initialized) {
            System.err.println("Store not initialized");
            return false;
        }
        long now = System.currentTimeMillis();
        if (timestamp < now - retentionPeriodMs) {
            System.err.println("data too old");
            return false;
        }
        try {
            String internedMetric = intern(metric);
            Map<String, String> internedTags = null;
            if (tags != null) {
                internedTags = new HashMap<>();
                for (Map.Entry<String, String> entry : tags.entrySet()) {
                    internedTags.put(intern(entry.getKey()), intern(entry.getValue()));
                }
            }

            DataPoint dataPoint = new DataPoint(timestamp, internedMetric, value, internedTags);

            metricStore.putIfAbsent(metric, new TreeMap<>());
            lockMap.putIfAbsent(metric, new ReentrantReadWriteLock(true));

            ReentrantReadWriteLock lock = lockMap.get(metric);
            lock.writeLock().lock();
            try {
                metricStore.get(metric).computeIfAbsent(timestamp, k -> new ArrayList<>()).add(dataPoint);
            } finally {
                lock.writeLock().unlock();
            }

            writeQueue.offer(dataPoint);
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public List<DataPoint> query(String metric, long timeStart, long timeEnd, Map<String, String> filters) {
        if (!initialized) {
            System.err.println("Store not initialized");
            return Collections.emptyList();
        }
        long now = System.currentTimeMillis();
        if (timeStart < now - retentionPeriodMs) {
            System.err.println("data too old");
            return Collections.emptyList();
        }
        List<DataPoint> result = new ArrayList<>();

        NavigableMap<Long, List<DataPoint>> metricData = metricStore.get(metric);
        if (metricData == null) return result;

        ReentrantReadWriteLock lock = lockMap.get(metric);
        if (lock == null) return result;

        lock.readLock().lock();
        try {
            for (Map.Entry<Long, List<DataPoint>> entry : metricData.subMap(timeStart, true, timeEnd, false).entrySet()) {
                for (DataPoint dp : entry.getValue()) {
                    if (filters == null || filters.entrySet().stream().allMatch(f -> f.getValue().equals(dp.getTags().get(f.getKey())))) {
                        result.add(dp);
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }

        return result;
    }

    @Override
    public synchronized boolean initialize() {
        if (initialized) return true;

        try {
            File file = new File(DATA_FILE_PATH);
            if (!file.exists()) file.createNewFile();

            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                long cutoffTime = System.currentTimeMillis() - retentionPeriodMs;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;

                    DataPoint dp = objectMapper.readValue(line, DataPoint.class);
                    if (dp.getTimestamp() < cutoffTime) continue;

                    String internedMetric = intern(dp.getMetric());
                    Map<String, String> internedTags = null;

                    if (dp.getTags() != null) {
                        internedTags = new HashMap<>();
                        for (Map.Entry<String, String> entry : dp.getTags().entrySet()) {
                            internedTags.put(intern(entry.getKey()), intern(entry.getValue()));
                        }
                    }
                    DataPoint internedDp = new DataPoint(dp.getTimestamp(), internedMetric, dp.getValue(), internedTags);

                    metricStore.putIfAbsent(internedMetric, new TreeMap<>());
                    lockMap.putIfAbsent(internedMetric, new ReentrantReadWriteLock(true));
                    metricStore.get(internedMetric).computeIfAbsent(internedDp.getTimestamp(), k -> new ArrayList<>()).add(internedDp);
                }
            }
            writer = new BufferedWriter(new FileWriter(file, true));
            startWriterThread();
            startCleanupThread();
            startFileCleanupScheduler();

            initialized = true;
            return true;

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public synchronized boolean shutdown() {
        if (!initialized) return false;

        try {
            writerRunning = false;
            if (writerThread != null) {
                writerThread.interrupt();
                writerThread.join();
            }
            if (cleanupThread != null) {
                cleanupRunning = false;
                cleanupThread.interrupt();
                cleanupThread.join();
            }

            fileCleanupExecutor.shutdownNow();
            if (writer != null) {
                fileLock.lock();
                try {
                    writer.close();
                } finally {
                    fileLock.unlock();
                }
            }

            stringCache.clear();
            metricStore.clear();
            lockMap.clear();

            return true;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    private void startWriterThread() {
        writerRunning = true;
        writerThread = new Thread(() -> {
            List<DataPoint> batch = new ArrayList<>();
            long lastFlushTime = System.currentTimeMillis();

            while (true) {
                try {
                    DataPoint dp = writeQueue.poll(FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
                    if (dp != null) batch.add(dp);

                    if (batch.size() >= BATCH_SIZE ||
                            (System.currentTimeMillis() - lastFlushTime > FLUSH_INTERVAL_MS && !batch.isEmpty())) {
                        flushBatch(batch);
                        batch.clear();
                        lastFlushTime = System.currentTimeMillis();
                    }

                    if (!writerRunning && writeQueue.isEmpty()) break;

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }

            flushBatch(batch); // final flush
        }, "WriterThread");

        writerThread.start();
    }

    private void startCleanupThread() {
        cleanupRunning = true;
        cleanupThread = new Thread(() -> {
            while (cleanupRunning) {
                try {
                    long cutoffTime = System.currentTimeMillis() - retentionPeriodMs;

                    for (String metric : metricStore.keySet()) {
                        ReentrantReadWriteLock lock = lockMap.get(metric);
                        if (lock == null) continue;

                        lock.writeLock().lock();
                        try {
                            NavigableMap<Long, List<DataPoint>> data = metricStore.get(metric);
                            if (data != null) {
                                data.headMap(cutoffTime, false).clear();
                            }
                        } finally {
                            lock.writeLock().unlock();
                        }
                    }

                    Thread.sleep(CLEANUP_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.out.println("Memory Data cleaned");
            }
        }, "CleanupThread");

        cleanupThread.start();
    }

    private void cleanupOldDataFromFile() {
        fileLock.lock();
        try {
            File inputFile = new File(DATA_FILE_PATH);
            File tempFile = new File(DATA_FILE_PATH + ".tmp");

            long cutoff = System.currentTimeMillis() - retentionPeriodMs;

            try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
                 BufferedWriter tempWriter = new BufferedWriter(new FileWriter(tempFile))) {

                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.trim().isEmpty()) continue;
                    DataPoint dp = objectMapper.readValue(line, DataPoint.class);
                    if (dp.getTimestamp() >= cutoff) {
                        tempWriter.write(objectWriter.writeValueAsString(dp));
                        tempWriter.newLine();
                    }
                }
            }

            if (!inputFile.delete() || !tempFile.renameTo(inputFile)) {
                System.err.println("File cleanup failed");
            }

            writer = new BufferedWriter(new FileWriter(inputFile, true));
            System.out.println("old data in file cleaned");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            fileLock.unlock();
        }
    }

    private void startFileCleanupScheduler() {
        long delay = computeInitialDelay(1, 0); // Run daily at 1:00 AM
        fileCleanupExecutor.scheduleAtFixedRate(this::cleanupOldDataFromFile, delay, TimeUnit.DAYS.toMillis(1), TimeUnit.MILLISECONDS);
    }

    private long computeInitialDelay(int hour, int minute) {
        Calendar nextRun = Calendar.getInstance();
        nextRun.set(Calendar.HOUR_OF_DAY, hour);
        nextRun.set(Calendar.MINUTE, minute);
        nextRun.set(Calendar.SECOND, 0);
        nextRun.set(Calendar.MILLISECOND, 0);

        Calendar now = Calendar.getInstance();
        if (now.after(nextRun)) {
            nextRun.add(Calendar.DAY_OF_MONTH, 1); // schedule for tomorrow
        }

        return nextRun.getTimeInMillis() - now.getTimeInMillis();
    }
    private void flushBatch(List<DataPoint> batch) {
        if (writer == null) return;
        fileLock.lock();
        try {
            for (DataPoint dp : batch) {
                writer.write(objectWriter.writeValueAsString(dp));
                writer.newLine();
            }
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            fileLock.unlock();
        }
    }

    private String intern(String str) {
        if (str == null) return null;
        return stringCache.computeIfAbsent(str, s -> s);
    }

}
