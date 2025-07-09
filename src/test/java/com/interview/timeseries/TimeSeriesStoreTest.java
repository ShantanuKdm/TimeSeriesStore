package com.interview.timeseries;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TimeSeriesStoreTest {
    
    private TimeSeriesStore store;

    @Before
    public void setUp() {
        store = new TimeSeriesStoreImpl("data.jsonl",TimeUnit.HOURS.toMillis(24));
        store.initialize();
    }

    @After
    public void tearDown() {
        store.shutdown();
//        new File("test_data.json").delete();
    }
    
    @Test
    public void testInsertAndQueryBasic() {
        // Insert test data
        long now = System.currentTimeMillis();
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "server1");
        
        assertTrue(store.insert(now, "cpu.usage", 45.2, tags));
        
        // Query for the data
        List<DataPoint> results = store.query("cpu.usage", now, now + 1, tags);
        
        // Verify
        assertEquals(1, results.size());
        assertEquals(now, results.get(0).getTimestamp());
        assertEquals("cpu.usage", results.get(0).getMetric());
        assertEquals(45.2, results.get(0).getValue(), 0.001);
        assertEquals("server1", results.get(0).getTags().get("host"));
    }
    
    @Test
    public void testQueryTimeRange() {
        // Insert test data at different times
        long start = System.currentTimeMillis();
        Map<String, String> tags = new HashMap<>();
        tags.put("host", "server1");
        
        store.insert(start, "cpu.usage", 45.2, tags);
        store.insert(start + 1000, "cpu.usage", 48.3, tags);
        store.insert(start + 2000, "cpu.usage", 51.7, tags);
        
        // Query for a subset
        List<DataPoint> results = store.query("cpu.usage", start, start + 1500, tags);
        
        // Verify
        assertEquals(2, results.size());
    }
    
    @Test
    public void testQueryWithFilters() {
        // Insert test data with different tags
        long now = System.currentTimeMillis();
        
        Map<String, String> tags1 = new HashMap<>();
        tags1.put("host", "server1");
        tags1.put("datacenter", "us-west");
        
        Map<String, String> tags2 = new HashMap<>();
        tags2.put("host", "server2");
        tags2.put("datacenter", "us-west");
        
        store.insert(now, "cpu.usage", 45.2, tags1);
        store.insert(now, "cpu.usage", 42.1, tags2);
        
        // Query with filter on datacenter
        Map<String, String> filter = new HashMap<>();
        filter.put("datacenter", "us-west");
        
        List<DataPoint> results = store.query("cpu.usage", now, now + 1, filter);
        
        // Verify
        assertEquals(2, results.size());
        
        // Query with filter on host
        filter.clear();
        filter.put("host", "server1");
        
        results = store.query("cpu.usage", now, now + 1, filter);
        
        // Verify
        assertEquals(1, results.size());
        assertEquals("server1", results.get(0).getTags().get("host"));
    }

    @Test
    public void testConcurrentInserts() throws InterruptedException {
        int threadCount = 10;
        int insertsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        long baseTime = System.currentTimeMillis();
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < insertsPerThread; j++) {
                    store.insert(baseTime + j, "cpu.usage", Math.random() * 100, Map.of("host", "serverX"));
                }
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        List<DataPoint> results = store.query("cpu.usage", baseTime, baseTime + insertsPerThread + 1, Map.of("host", "serverX"));
        assertEquals(threadCount * insertsPerThread, results.size());
    }

    @Test
    public void testMemoryCapacity() {
        long base = System.currentTimeMillis();
        String metric = "mem.usage";
        int count = 100_000;
        for (int i = 0; i < count; i++) {
            long ts = base + i;
            assertTrue("Insert " + i, store.insert(ts, metric, i, null));
        }
        List<DataPoint> all = store.query(metric, base, base + count, null);
        assertEquals("Should store and retrieve 100k points", count, all.size());
    }

    @Test
    public void testQueryPerformance() {
        long base = System.currentTimeMillis();
        String metric = "net.bytes";
        int count = 50_000;
        for (int i = 0; i < count; i++) {
            assertTrue(store.insert(base + i, metric, i, null));
        }
        long start = System.nanoTime();
        List<DataPoint> res = store.query(metric, base, base + count, null);
        long durationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertEquals("Count mismatch", count, res.size());
        assertTrue("Query should complete under 100ms but took " + durationMs + "ms",
                durationMs < 100);
    }
}
