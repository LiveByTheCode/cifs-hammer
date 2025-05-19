package com.example;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FileWriteHammer {

    private static final File BASE_DIR = new File("/images/test/session_" + System.currentTimeMillis());
    private static final int THREAD_COUNT = 4;
    private static final int ITERATIONS_PER_THREAD = 2500; // 4 threads * 2500 = 10,000 total
    private static final int APPEND_COUNT = 16;
    private static final boolean SHOW_PROGRESS = true;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Map<String, Integer> errorCounts = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        File workingDir = new File(BASE_DIR, "working");
        File destDir = new File(BASE_DIR, "dest");

        FileUtils.forceMkdir(workingDir);
        FileUtils.forceMkdir(destDir);

        System.out.println("📁 Shared test directory: " + BASE_DIR.getAbsolutePath());

        Thread[] threads = new Thread[THREAD_COUNT];
        for (int t = 0; t < THREAD_COUNT; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> runWorker(threadId, workingDir, destDir));
            threads[t].start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        System.out.println("\n All threads completed.");
        printSummary();
    }

    private static void runWorker(int threadId, File workingDir, File destDir) {
        try {
            for (int i = 0; i < ITERATIONS_PER_THREAD; i++) {
                String filePrefix = "t" + threadId + "_i" + i;
                File original = new File(BASE_DIR, filePrefix + "_original.txt");
                File workingFile = new File(workingDir, "working.txt");

                // Step 1: Create original file
                FileUtils.writeStringToFile(original, "initial content", StandardCharsets.UTF_8);

                // Step 2: Move it to destDir
                FileUtils.moveFileToDirectory(original, destDir, true);
                File moved = new File(destDir, original.getName());

                // Step 3: Rename it
                File renamed = new File(destDir, filePrefix + "_renamed.txt");
                FileUtils.moveFile(moved, renamed);

                // Step 4: Append 16 entries to shared working file
                LocalDateTime now = LocalDateTime.now();
                for (int j = 0; j < APPEND_COUNT; j++) {
                    String line = now.format(FORMATTER) + ",thread=" + threadId + ",write=" + j + ",file=" + renamed.getName() + "\r\n";
                    FileUtils.writeStringToFile(workingFile, line, StandardCharsets.UTF_8, true);
                }

                if (SHOW_PROGRESS && i % 250 == 0) {
                    System.out.println("Thread " + threadId + " progress: iteration " + i);
                }
            }
        } catch (Exception e) {
            String key = e.getClass().getSimpleName();
            errorCounts.merge(key, 1, Integer::sum);

            System.err.println("\n ERROR in thread " + threadId);
            logDetailedException(e);

            System.err.println("\n Exiting due to failure in thread " + threadId);
            printSummary();
            System.exit(1);
        }
    }

    private static void logDetailedException(Throwable e) {
        System.err.println("• Exception: " + e.getClass().getName());
        System.err.println("• Message  : " + e.getMessage());
        Throwable cause = e.getCause();
        while (cause != null) {
            System.err.println("↳ Caused by: " + cause.getClass().getName() + " - " + cause.getMessage());
            cause = cause.getCause();
        }
        e.printStackTrace(System.err);
    }

    private static void printSummary() {
        System.out.println("\n📊 Exception Summary:");
        errorCounts.forEach((key, count) ->
                System.out.printf("  • %-30s : %d\n", key, count)
        );
    }
}