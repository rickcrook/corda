package net.corda.client.rpc.internal

import net.corda.client.rpc.*
import net.corda.core.messaging.CordaRPCOps
import net.corda.core.messaging.DataFeed
import net.corda.core.utilities.NetworkHostAndPort
import net.corda.core.utilities.seconds
import net.corda.nodeapi.exceptions.RejectedCommandException
import org.apache.activemq.artemis.api.core.ActiveMQConnectionTimedOutException
import org.apache.activemq.artemis.api.core.ActiveMQSecurityException
import org.apache.activemq.artemis.api.core.ActiveMQUnBlockedException
import org.slf4j.LoggerFactory
import rx.Observable
import java.lang.IllegalArgumentException
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * Utilities that reconnect when the node or the fail.
 * There is no guarantee that observations will not be lost or flows that didn't receive any result were started or not.
 *
 */
object ReconnectingRPCClient {
    private val log = LoggerFactory.getLogger(ReconnectingRPCClient::class.java)

    /**
     * Helper class useful for reconnecting to a Node.
     */
    data class ReconnectingRPCConnection(
            val nodeHostAndPort: NetworkHostAndPort,
            val username: String,
            val password: String
    ) : RPCConnection<CordaRPCOps> {
        private var currentRPCConnection: CordaRPCConnection? = null

        enum class CurrentState {
            UNCONNECTED, CONNECTED, CONNECTING, CLOSED, DIED
        }

        private var currentState = CurrentState.UNCONNECTED

        constructor(nodeHostAndPort: NetworkHostAndPort, username: String, password: String, existingRPCConnection: CordaRPCConnection) : this(nodeHostAndPort, username, password) {
            this.currentRPCConnection = existingRPCConnection
            currentState = CurrentState.CONNECTED
        }

        private val current: CordaRPCConnection
            @Synchronized get() = when (currentState) {
                CurrentState.CONNECTED -> currentRPCConnection!!
                CurrentState.UNCONNECTED, CurrentState.CLOSED -> {
                    currentState = CurrentState.CONNECTING
                    currentRPCConnection = establishConnectionWithRetry()
                    currentState = CurrentState.CONNECTED
                    currentRPCConnection!!
                }
                CurrentState.CONNECTING, CurrentState.DIED -> throw IllegalArgumentException("Illegal state")
            }

        /**
         * Called on external error.
         * Will block until the connection is established again.
         */
        @Synchronized
        fun error(e: Throwable) {
            currentState = CurrentState.DIED
            //TODO - handle error cases
            log.error("Reconnecting to ${this.nodeHostAndPort} due to error: ${e.message}")
            currentState = CurrentState.CONNECTING
            currentRPCConnection = establishConnectionWithRetry()
            currentState = CurrentState.CONNECTED
        }

        // TODO - use exponential backoff for the retry interval
        private tailrec fun establishConnectionWithRetry(retryInterval: Duration = 5.seconds): CordaRPCConnection {
            log.info("Connecting to: $nodeHostAndPort")
            try {
                return CordaRPCClient(
                        nodeHostAndPort, CordaRPCClientConfiguration(connectionMaxRetryInterval = retryInterval)
                ).start(username, password).also {
                    // Check connection is truly operational before returning it.
                    require(it.proxy.nodeInfo().legalIdentitiesAndCerts.isNotEmpty()) {
                        "Could not establish connection to ${nodeHostAndPort}."
                    }
                    log.info("Connection successfully established with: ${nodeHostAndPort}")
                }
            } catch (ex: Exception) {
                when (ex) {
                    is ActiveMQSecurityException -> {
                        // Happens when incorrect credentials provided.
                        // It can happen at startup as well when the credentials are correct.
                        // TODO - add a counter to only retry 2-3 times on security exceptions,
                    }
                    is RPCException -> {
                        // Deliberately not logging full stack trace as it will be full of internal stacktraces.
                        log.info("Exception upon establishing connection: ${ex.message}")
                    }
                    is ActiveMQConnectionTimedOutException -> {
                        // Deliberately not logging full stack trace as it will be full of internal stacktraces.
                        log.info("Exception upon establishing connection: ${ex.message}")
                    }
                    is ActiveMQUnBlockedException -> {
                        // Deliberately not logging full stack trace as it will be full of internal stacktraces.
                        log.info("Exception upon establishing connection: ${ex.message}")
                    }
                    else -> {
                        log.info("Unknown exception upon establishing connection.", ex)
                    }
                }
            }

            // Could not connect this time round - pause before giving another try.
            Thread.sleep(retryInterval.toMillis())
            return establishConnectionWithRetry(retryInterval)
        }

        override val proxy: CordaRPCOps
            get() = current.proxy

        fun reconnectingProxy(observerNotifier: ObserverNotifier? = null): ReconnectingCordaRPCOps = ReconnectingCordaRPCOps(this, observerNotifier)

        override val serverProtocolVersion
            get() = current.serverProtocolVersion

        @Synchronized
        override fun notifyServerAndClose() {
            currentState = CurrentState.CLOSED
            currentRPCConnection!!.notifyServerAndClose()
        }

        @Synchronized
        override fun forceClose() {
            currentState = CurrentState.CLOSED
            currentRPCConnection!!.forceClose()
        }

        @Synchronized
        override fun close() {
            currentState = CurrentState.CLOSED
            currentRPCConnection!!.close()
        }
    }

    class ReconnectingCordaRPCOps(reconnectingRPCConnection: ReconnectingRPCConnection, observerNotifier: ObserverNotifier?) : CordaRPCOps by proxy(reconnectingRPCConnection, observerNotifier) {
        private companion object {
            private fun proxy(reconnectingRPCConnection: ReconnectingRPCConnection, observerNotifier: ObserverNotifier?): CordaRPCOps {
                return Proxy.newProxyInstance(
                        this::class.java.classLoader,
                        arrayOf(CordaRPCOps::class.java),
                        ErrorInterceptingHandler(reconnectingRPCConnection, observerNotifier)) as CordaRPCOps
            }
        }

        private class ErrorInterceptingHandler(val reconnectingRPCConnection: ReconnectingRPCConnection, val observerNotifier: ObserverNotifier?) : InvocationHandler {
            override fun invoke(proxy: Any?, method: Method?, args: Array<out Any>?): Any? = try {
                val delegate = reconnectingRPCConnection.proxy
                log.info("Invoking RPC $method...")
                val isDataFeed = method!!.returnType == DataFeed::class.java

                // Intercept the data feed methods
                if (isDataFeed && observerNotifier != null) {
                    val initialFeed = method.invoke(delegate, *(args ?: emptyArray())) as DataFeed<Any, Any?>
                    val observable = ReconnectingObservable(reconnectingRPCConnection, observerNotifier, initialFeed) {
                        // This handles reconnecting and creates new feeds.
                        this.invoke(null, method, args) as DataFeed<Any, Any?>
                    }
                    initialFeed.copy(updates = observable)
                } else {
                    method.invoke(delegate, *(args ?: emptyArray()))
                }
            } catch (e: InvocationTargetException) {
                when (e.targetException) {
                    is RejectedCommandException -> {
                        log.error("Node is being shutdown. Operation ${method!!.name} rejected. Retrying when node is up.", e)
                        reconnectingRPCConnection.error(e)
                        this.invoke(null, method, args)
                    }
                    is ConnectionFailureException -> {
                        log.error("Failed to perform operation ${method!!.name}. Connection dropped.", e)
                        reconnectingRPCConnection.error(e)
                        if (method.name.startsWith("startFlow") || method.name.startsWith("startTrackedFlow")) {
                            // Don't retry flows
                            throw CouldNotStartFlowException()
                        } else {
                            this.invoke(proxy, method, args)
                        }
                    }
                    is RPCException -> {
                        log.error("Failed to perform operation ${method!!.name}. RPCException.", e)
                        reconnectingRPCConnection.error(e)
                        Thread.sleep(1000)
                        this.invoke(proxy, method, args)
                    }
                    else -> {
                        throw CouldNotStartFlowException(e.targetException)
                    }
                }
            }
        }
    }

    class CouldNotStartFlowException(cause: Throwable? = null) : RPCException("Could not start flow as connection failed", cause)

    class ReconnectingObservable<A, B>(
            private val reconnectingRPCConnection: ReconnectingRPCConnection,
            private val observerNotifier: ObserverNotifier,
            val initial: DataFeed<A, B>,
            val createDataFeed: () -> DataFeed<A, B>
    ) : Observable<B>(null) {

        /**
         * This is a blocking function.
         */
        fun synchSubscribeWithReconnect(onNext: (B) -> Unit) {
            var subscriptionError: Throwable?

            try {
                val subscription = initial.updates.subscribe(onNext, observerNotifier::fail, observerNotifier::stop)
                subscriptionError = observerNotifier.await()
                subscription.unsubscribe()
            } catch (e: Exception) {
                log.error("Failed to register subscriber .", e)
                subscriptionError = e
            }

            // In case there was no exception the observer has finished gracefully.
            if (subscriptionError == null) return

            // Only continue if the subscription failed.
            reconnectingRPCConnection.error(subscriptionError)
            log.info("Recreating data feed.")

            val newObservable = createDataFeed().updates as ReconnectingObservable<*, B>
            return newObservable.synchSubscribeWithReconnect(onNext)
        }
    }

    fun <B> Observable<B>.asReconnecting() = this as ReconnectingObservable<*, B>

    /**
     * Utility to externally control a registered subscription.
     */
    class ObserverNotifier {
        private val terminated = LinkedBlockingQueue<Optional<Throwable>>(1)

        fun stop() = terminated.put(Optional.empty())
        fun fail(e: Throwable) = terminated.put(Optional.of(e))

        /**
         * Returns null if the observation ended successfully.
         */
        internal fun await(): Throwable? = terminated.poll(100, TimeUnit.MINUTES).orElse(null)

        internal fun reset(): ObserverNotifier = this.also { it.terminated.clear() }
    }
}
