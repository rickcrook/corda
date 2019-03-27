package net.corda.node.services.rpc

import net.corda.client.rpc.internal.ReconnectingRPCClient
import net.corda.client.rpc.internal.ReconnectingRPCClient.ObserverNotifier
import net.corda.client.rpc.internal.ReconnectingRPCClient.ReconnectingRPCConnection
import net.corda.client.rpc.internal.ReconnectingRPCClient.asReconnecting
import net.corda.core.contracts.Amount
import net.corda.core.flows.StateMachineRunId
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.transpose
import net.corda.core.messaging.StateMachineUpdate
import net.corda.core.node.services.Vault
import net.corda.core.node.services.vault.PageSpecification
import net.corda.core.node.services.vault.QueryCriteria
import net.corda.core.node.services.vault.builder
import net.corda.core.utilities.OpaqueBytes
import net.corda.core.utilities.contextLogger
import net.corda.core.utilities.getOrThrow
import net.corda.finance.contracts.asset.Cash
import net.corda.finance.flows.CashIssueAndPaymentFlow
import net.corda.finance.schemas.CashSchemaV1
import net.corda.node.services.Permissions
import net.corda.testing.core.DUMMY_BANK_A_NAME
import net.corda.testing.core.DUMMY_BANK_B_NAME
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.NodeHandle
import net.corda.testing.driver.OutOfProcess
import net.corda.testing.driver.driver
import net.corda.testing.driver.internal.OutOfProcessImpl
import net.corda.testing.node.User
import net.corda.testing.node.internal.FINANCE_CORDAPPS
import org.junit.Test
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.concurrent.thread
import kotlin.math.absoluteValue
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * This is a slow test.
 */
class RpcReconnectTests {

    companion object {
        private val log = contextLogger()
    }

    /**
     * This test showcases a pattern for making the RPC client reconnect.
     *
     * Note that during node failure events can be lost and starting flows can become unreliable.
     * The purpose of this test and utilities is to handle reconnects and make best efforts to retry.
     *
     * This test runs flows in a loop and in the background kills the node or restarts it.
     * Also the RPC connection is made through a proxy that introduces random latencies and is also periodically killed.
     */
    @Test
    fun `Test that the RPC client is able to reconnect and proceed after node failure, restart, or connection reset`() {
        val nrOfFlowsToRun = 450 // Takes around 5 minutes.
        val nodeRunningTime = { Random().nextInt(12000) + 8000 }

        val demoUser = User("demo", "demo", setOf(Permissions.all()))

        val nodePort = 20006
        val proxyPort = 20007

        // When this reaches 0 - the test will end.
        val flowsCountdownLatch = CountDownLatch(nrOfFlowsToRun + 1)

        // These are the expected progress steps for the CashIssueAndPayFlow.
        val expectedProgress = listOf(
                "Starting",
                "Issuing cash",
                "Generating transaction",
                "Signing transaction",
                "Finalising transaction",
                "Broadcasting transaction to participants",
                "Paying recipient",
                "Generating anonymous identities",
                "Generating transaction",
                "Signing transaction",
                "Finalising transaction",
                "Requesting signature by notary service",
                "Requesting signature by Notary service",
                "Validating response from Notary service",
                "Broadcasting transaction to participants",
                "Done"
        )

        driver(DriverParameters(cordappsForAllNodes = FINANCE_CORDAPPS, startNodesInProcess = false, inMemoryDB = false)) {
            fun startBankA() = startNode(providedName = DUMMY_BANK_A_NAME, rpcUsers = listOf(demoUser), customOverrides = mapOf("rpcSettings.address" to "localhost:$nodePort"))

            var (bankA, bankB) = listOf(
                    startBankA(),
                    startNode(providedName = DUMMY_BANK_B_NAME, rpcUsers = listOf(demoUser))
            ).transpose().getOrThrow()

            val notary = defaultNotaryIdentity
            val bankAConnection = ReconnectingRPCConnection(bankA.rpcAddress.copy(port = proxyPort), demoUser.username, demoUser.password)
            val tcpProxy = RandomFailingProxy(serverPort = proxyPort, remotePort = nodePort).start()

            // Start nrOfFlowsToRun flows in the background.
            var flowProgressEvents: Map<StateMachineRunId, List<String>>? = null
            var lostFlows: Int = 0
            thread(name = "Flow feeder") {
                val (_flowProgressEvents, _lostFlows) = runTestFlows(bankAConnection, nrOfFlowsToRun, flowsCountdownLatch, bankB, notary)
                flowProgressEvents = _flowProgressEvents
                lostFlows = _lostFlows
            }

            // Observe the vault.
            val vaultObserverNotifier = ObserverNotifier()
            val vaultEvents = Collections.synchronizedList(mutableListOf<Vault.Update<Cash.State>>())
            thread(name = "Vault observer") {
                val feed = bankAConnection.reconnectingProxy(vaultObserverNotifier).vaultTrackByWithPagingSpec(
                        Cash.State::class.java,
                        QueryCriteria.VaultQueryCriteria(),
                        PageSpecification(1, 1)
                )

                feed.updates.asReconnecting().synchSubscribeWithReconnect { update: Vault.Update<Cash.State> ->
                    log.info("vault update produced ${update.produced.map { it.state.data.amount }} consumed ${update.consumed.map { it.ref }}")
                    vaultEvents.add(update)
                }
            }

            // Observe the stateMachine.
            val stateMachineNotifier = ObserverNotifier()
            val stateMachineEvents = Collections.synchronizedList(mutableListOf<StateMachineUpdate>())
            thread(name = "State machine observer") {
                val feed = bankAConnection.reconnectingProxy(stateMachineNotifier).stateMachinesFeed()
                feed.updates.asReconnecting().synchSubscribeWithReconnect { update ->
                    log.info(update.toString())
                    stateMachineEvents.add(update)
                }
            }

            // While the flows are running, randomly apply a different failure scenario.
            val nrRestarts = AtomicInteger()
            thread(name = "Node killer") {
                while (true) {
                    if (flowsCountdownLatch.count == 0L) break

                    // Let the node run for a random time interval.
                    nodeRunningTime().also { ms ->
                        log.info("Running node for ${ms / 1000} s.")
                        Thread.sleep(ms.toLong())
                    }

                    if (flowsCountdownLatch.count == 0L) break

                    when (Random().nextInt().rem(6).absoluteValue) {
                        0 -> {
                            log.info("Forcefully killing node and proxy.")
                            (bankA as OutOfProcessImpl).onStopCallback()
                            (bankA as OutOfProcess).process.destroyForcibly()
                            tcpProxy.stop()
                            bankA = startBankA().get()
                            tcpProxy.start()
                        }
                        1 -> {
                            log.info("Forcefully killing node.")
                            (bankA as OutOfProcessImpl).onStopCallback()
                            (bankA as OutOfProcess).process.destroyForcibly()
                            bankA = startBankA().get()
                        }
                        2 -> {
                            log.info("Shutting down node.")
                            bankA.stop()
                            tcpProxy.stop()
                            bankA = startBankA().get()
                            tcpProxy.start()
                        }
                        3, 4 -> {
                            log.info("Killing proxy.")
                            tcpProxy.stop()
                            Thread.sleep(Random().nextInt(5000).toLong())
                            tcpProxy.start()
                        }
                        5 -> {
                            log.info("Dropping connection.")
                            tcpProxy.failConnection()
                        }
                    }
                    nrRestarts.incrementAndGet()
                }
            }

            // Wait until all flows have been started.
            flowsCountdownLatch.await()

            // Wait for all events to come in.
            Thread.sleep(5000)

            // Stop the vault observer.
            vaultObserverNotifier.stop()
            stateMachineNotifier.stop()
            Thread.sleep(1000)

            val nrFailures = nrRestarts.get()
            log.info("Checking results after $nrFailures restarts.")

            // The progress status for each flow can only miss the last events, because the node might have been killed.
            val missingProgressEvents = flowProgressEvents!!.filterValues { expectedProgress.subList(0, it.size) != it }
            assertTrue(missingProgressEvents.isEmpty(), "The flow progress tracker is missing events: $missingProgressEvents")

            // Check that enough vault events were received.
            // This check is fuzzy because events can go missing during node restarts.
            // Ideally there should be nrOfFlowsToRun events receive but some might get lost for each restart.
            assertTrue(vaultEvents!!.size + nrFailures * 2 >= nrOfFlowsToRun, "Not all vault events were received")

            // Query the vault and check that states were created for all confirmed flows

            val allCashStates = bankAConnection.proxy
                    .vaultQueryByWithPagingSpec(Cash.State::class.java, QueryCriteria.VaultQueryCriteria(status = Vault.StateStatus.CONSUMED), PageSpecification(1, 10000))
                    .states

            assertEquals(nrOfFlowsToRun, allCashStates.size, "Not all flows were executed successfully")

            // Check that no flow was triggered twice.
            val duplicates = allCashStates.groupBy { it.state.data.amount }.filterValues { it.size > 1 }
            assertTrue(duplicates.isEmpty(), "${duplicates.size} flows were retried illegally.")

            log.info("SM EVENTS: ${stateMachineEvents!!.size}")
            // State machine events are very likely to get lost more often because they seem to be sent with a delay.
            assertTrue(stateMachineEvents.count { it is StateMachineUpdate.Added } > nrOfFlowsToRun / 2, "Too many Added state machine events lost.")
            assertTrue(stateMachineEvents.count { it is StateMachineUpdate.Removed } > nrOfFlowsToRun / 2, "Too many Removed state machine events lost.")

            assertEquals(nrOfFlowsToRun, flowProgressEvents!!.size + lostFlows, "Not all flows were triggered")

            tcpProxy.close()
            bankAConnection.forceClose()
        }
    }

    @Synchronized
    fun MutableMap<StateMachineRunId, MutableList<String>>.addEvent(id: StateMachineRunId, progress: String?): Boolean {
        return getOrPut(id) { mutableListOf() }.let { if (progress != null) it.add(progress) else false }
    }

    /**
     * This function runs [nrOfFlowsToRun] flows and returns the progress of each one of these flows.
     */
    private fun runTestFlows(bankAConnection: ReconnectingRPCConnection, nrOfFlowsToRun: Int, flowsCountdownLatch: CountDownLatch, bankB: NodeHandle, notary: Party): Pair<Map<StateMachineRunId, List<String>>, Int> {
        val baseAmount = Amount.parseCurrency("0 USD")
        val issuerRef = OpaqueBytes.of(0x01)

        val flowProgressEvents: MutableMap<StateMachineRunId, MutableList<String>> = mutableMapOf()
        val flowsWithNoProgress = AtomicInteger()

        fun runFlow(i: Int) {
            val flowHandle = bankAConnection.reconnectingProxy().startTrackedFlowDynamic(
                    CashIssueAndPaymentFlow::class.java,
                    baseAmount.plus(Amount.parseCurrency("$i USD")),
                    issuerRef,
                    bankB.nodeInfo.legalIdentities.first(),
                    false,
                    notary
            )
            val flowId = flowHandle.id
            log.info("Started flow $i with flowId: $flowId, cnt: ${flowsCountdownLatch.count}")
            flowProgressEvents.addEvent(flowId, null)

            // No reconnecting possible.
            flowHandle.progress.subscribe { prog ->
                flowProgressEvents.addEvent(flowId, prog)
                log.info("Progress $flowId : $prog")
            }
        }

        val retryThreads = mutableListOf<Thread>()
        for (i in (1..nrOfFlowsToRun)) {
            log.info("Starting flow $i")
            try {
                runFlow(i)
            } catch (e: ReconnectingRPCClient.CouldNotStartFlowException) {
                log.error("Couldn't start flow $i")
                retryThreads += thread(name = "RetryFlow $i") {
                    // This showcases a pattern that can be applied to decide if a flow needs retriggering.

                    // Wait for the flow to execute if it was actually triggered.
                    Thread.sleep(5000)

                    // Query for a state that is the result of this flow
                    val criteria = QueryCriteria.VaultCustomQueryCriteria(builder { CashSchemaV1.PersistentCashState::pennies.equal(i.toLong() * 100) }, status = Vault.StateStatus.ALL)
                    val results = bankAConnection.reconnectingProxy().vaultQueryByCriteria(criteria, Cash.State::class.java)

                    // Retry when the output state is missing
                    if (results.states.isEmpty()) {
                        log.info("Retrying flow $i")
                        // TODO  - make this recursive to check for reconnects
                        runFlow(i)
                    } else {
                        log.info("Failed flow $i executed successfully.")
                        flowsWithNoProgress.incrementAndGet()
                    }
                }
            } finally {
                flowsCountdownLatch.countDown()
            }
        }

        for (retryThread in retryThreads) {
            retryThread.join()
        }

        flowsCountdownLatch.countDown()
        return flowProgressEvents to flowsWithNoProgress.get()
    }

    /**
     * Simple proxy that can be restarted and introduces random latencies.
     * This also acts as a mock load balancer.
     */
    class RandomFailingProxy(val serverPort: Int, val remotePort: Int) {
        private val threadPool = Executors.newCachedThreadPool()
        private val stopCopy = AtomicBoolean(false)
        private var currentServerSocket: ServerSocket? = null
        private val rnd = ThreadLocal.withInitial { Random() }

        fun start(): RandomFailingProxy {
            stopCopy.set(false)
            currentServerSocket = ServerSocket(serverPort)
            threadPool.execute {
                try {
                    currentServerSocket.use { serverSocket ->
                        while (!stopCopy.get() && !serverSocket!!.isClosed) {
                            handleConnection(serverSocket.accept())
                        }
                    }
                } catch (e: SocketException) {
                    // The Server socket could be closed
                }
            }
            return this
        }

        private fun handleConnection(socket: Socket) {
            threadPool.execute {
                socket.use { _ ->
                    try {
                        Socket("localhost", remotePort).use { target ->
                            // send message to node
                            threadPool.execute {
                                try {
                                    socket.getInputStream().flakeyCopyTo(target.getOutputStream())
                                } catch (e: IOException) {
                                    // Thrown when the connection to the target server dies.
                                }
                            }
                            target.getInputStream().flakeyCopyTo(socket.getOutputStream())
                        }
                    } catch (e: IOException) {
                        // Thrown when the connection to the target server dies.
                    }
                }
            }
        }

        fun stop(): RandomFailingProxy {
            stopCopy.set(true)
            currentServerSocket?.close()
            return this
        }

        private val failOneConnection = AtomicBoolean(false)
        fun failConnection() {
            failOneConnection.set(true)
        }

        fun close() {
            try {
                stop()
                threadPool.shutdownNow()
            } catch (e: Exception) {
                // Nothing can be done.
            }
        }

        private fun InputStream.flakeyCopyTo(out: OutputStream, bufferSize: Int = DEFAULT_BUFFER_SIZE): Long {
            var bytesCopied: Long = 0
            val buffer = ByteArray(bufferSize)
            var bytes = read(buffer)
            while (bytes >= 0 && !stopCopy.get()) {
                // Introduce intermittent slowness.
                if (rnd.get().nextInt().rem(700) == 0) {
                    Thread.sleep(rnd.get().nextInt(2000).toLong())
                }
                if (failOneConnection.compareAndSet(true, false)) {
                    throw IOException("Randomly dropped one connection")
                }
                out.write(buffer, 0, bytes)
                bytesCopied += bytes
                bytes = read(buffer)
            }
            return bytesCopied
        }
    }
}
