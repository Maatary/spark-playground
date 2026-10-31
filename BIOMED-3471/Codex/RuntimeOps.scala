package biomed3471.codex

import biomed3471.codex.DataTypes.*

import java.lang.management.ManagementFactory
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.concurrent.{Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*
import scala.util.Using
import scala.util.control.NonFatal

/** Local JVM memory monitoring; it never reads investigation data or credentials. */
object RuntimeOps:

    /**
     * Checks the analysis JVM's launch-time allocation limits before Spark starts.
     *
     * The heap is limited to 2 GiB, with separate direct-memory, metaspace,
     * code-cache and thread-stack settings leaving room under the 4 GiB
     * resident-memory ceiling. These settings are prerequisites, not a claim
     * that the dataset's actual working set has already been measured.
     *
     * @throws IllegalArgumentException when the launch omits a required limit
     */
    def verifyJvmLimits(): Unit =
        require(Runtime.getRuntime.maxMemory() <= MaximumHeapBytes,
            "Launch the analysis JVM with --java-opt=-Xmx2g.")
        val supplied = ManagementFactory.getRuntimeMXBean.getInputArguments.asScala.toSet
        val missing  = RequiredJvmOptions.filterNot(supplied.contains)
        require(missing.isEmpty,
            s"Missing analysis JVM limits: ${missing.mkString(" ")}. See BIOMED-3471/Codex/README.md.")

    /**
     * Reads resident memory for the current analysis JVM on the user's macOS host.
     *
     * ps reports RSS in KiB. Only this process ID is queried; command lines,
     * environment variables and credentials are not read or logged.
     *
     * @param processId the analysis JVM process ID
     * @return its current resident memory in bytes
     */
    def residentBytes(processId: Long): Long =
        val process = new ProcessBuilder("/bin/ps", "-p", processId.toString, "-o", "rss=")
            .redirectErrorStream(true)
            .start()
        if !process.waitFor(3L, TimeUnit.SECONDS) then
            process.destroyForcibly()
            throw new IllegalStateException("Timed out reading analysis JVM resident memory.")
        val output = Using.resource(process.getInputStream) { stream =>
            new String(stream.readAllBytes(), StandardCharsets.UTF_8).trim
        }
        require(process.exitValue() == 0 && output.nonEmpty,
            "Could not read analysis JVM resident memory.")
        output.toLong * 1024L

    /**
     * Writes the sampled peak and run status to the local scratch directory.
     *
     * The report distinguishes a completed run from an interrupted one. It
     * records sampled RSS rather than claiming a bound on between-sample peaks.
     * It is written before a memory-triggered halt, which cannot run finally.
     *
     * @param monitor the monitor's process, peak and report path
     * @param status the current lifecycle or termination status
     */
    def writeMemoryReport(monitor: MemoryMonitor, status: String): Unit =
        val report =
            s"""timestamp=${Instant.now()}
               |processId=${monitor.processId}
               |status=$status
               |sampledPeakResidentBytes=${monitor.peakResidentBytes.get()}
               |residentStopBytes=$ResidentStopBytes
               |runtimeCeilingBytes=$RuntimeCeilingBytes
               |maximumHeapBytes=${Runtime.getRuntime.maxMemory()}
               |samplingPeriodMilliseconds=500
               |measurement=sampled process RSS; between-sample peaks are not measured
               |""".stripMargin
        Files.writeString(monitor.reportPath, report, StandardCharsets.UTF_8)
        ()

    /**
     * Records one RSS sample and stops when the 3.5 GiB threshold is observed.
     *
     * The stop threshold is 3.5 GiB, leaving 512 MiB of headroom for changes
     * between samples. A failed monitor also stops the run instead of allowing
     * unmonitored processing. Delta writes already committed remain readable;
     * dataset commit status is separate from the final runtime memory report.
     *
     * @param monitor the monitor associated with this JVM
     */
    def sampleMemory(monitor: MemoryMonitor): Unit =
        try
            val current = residentBytes(monitor.processId)
            monitor.peakResidentBytes.accumulateAndGet(current, (previous, next) => math.max(previous, next))
            if current >= ResidentStopBytes then
                try writeMemoryReport(monitor, "stopped: resident-memory threshold")
                finally
                    System.err.println(s"Stopping analysis at sampled RSS $current bytes; report: ${monitor.reportPath}")
                    Runtime.getRuntime.halt(137)
        catch
            case NonFatal(error) =>
                try writeMemoryReport(monitor, s"stopped: memory monitor failed (${error.getClass.getSimpleName})")
                finally
                    System.err.println(s"Stopping analysis because memory monitoring failed; report: ${monitor.reportPath}")
                    Runtime.getRuntime.halt(137)

    /**
     * Arms memory monitoring before Spark initialization and all data processing.
     *
     * Creates only a local report and a single sampling thread. The initial RSS
     * read must succeed before scheduling begins. No investigation data or S3
     * object is accessed by this operation.
     *
     * @param scratchPath the run's local spill and report directory
     * @return monitor state that main must stop in finally
     */
    def startMemoryMonitor(scratchPath: Path): MemoryMonitor =
        val processId = ProcessHandle.current().pid()
        val initial   = residentBytes(processId)
        require(initial < ResidentStopBytes, "Analysis JVM already exceeds its resident-memory stop threshold.")
        val scheduler = Executors.newSingleThreadScheduledExecutor()
        val monitor   = MemoryMonitor(scheduler, new AtomicLong(initial), scratchPath.resolve("memory-report.txt"), processId)
        try
            writeMemoryReport(monitor, "starting")
            val samplingTask = new Runnable:
                /**
                 * Samples the analysis JVM using the captured monitor state.
                 *
                 * Invoked by the scheduler every 500 ms. Updates the sampled
                 * peak and applies the resident-memory stop threshold without
                 * reading investigation data or collecting Spark results.
                 */
                override def run(): Unit = sampleMemory(monitor)
            scheduler.scheduleWithFixedDelay(samplingTask, 0L, 500L, TimeUnit.MILLISECONDS)
            monitor
        catch
            case NonFatal(error) =>
                scheduler.shutdownNow()
                throw error

    /**
     * Stops the sampler and records the final measured peak and lifecycle status.
     *
     * Called after Spark shutdown so monitoring covers initialization, reads,
     * shuffles, joins, writes and shutdown. It does not alter persisted datasets.
     *
     * @param monitor the sampler started for this run
     * @param status completed or failed, supplied by main
     */
    def stopMemoryMonitor(monitor: MemoryMonitor, status: String): Unit =
        monitor.scheduler.shutdown()
        if !monitor.scheduler.awaitTermination(5L, TimeUnit.SECONDS) then
            monitor.scheduler.shutdownNow()
        sampleMemory(monitor)
        writeMemoryReport(monitor, status)
