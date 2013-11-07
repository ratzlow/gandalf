package net.gandalf.syncfacade;

import org.junit.Assert;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.*;

/**
 * Simple design to face a client application with a synchronous facade even though the processing is handed off
 * to an async backend.
 *
 * The handover of the response from async backend to different client thread is done by an {@link Exchanger}.
 * The joining between the 2 arbitrary threads is done via a UID that is passed as part of the request.
 *
 * A {@link Future} will not do the job, since in a bigger system you don't know which component might full fill the
 * request and produces the result. A future expects you to dispatch to the response creation service.
 *
 * @author ratzlow@gmail.com
 * @since 2013-10-14
 */
public class SyncToAsyncTest {
    private static final Logger LOGGER = Logger.getLogger(SyncToAsyncTest.class);

    /** All requests waiting for a matching response will store their handle to the response here */
    private final ConcurrentMap<String, Exchanger<RequestResponsePair>> rendevouzPlace =
            new ConcurrentHashMap<String, Exchanger<RequestResponsePair>>();

    private final SyncClientServiceFacade service = new SyncClientServiceFacade();

    private final int callCount = 1000;
    private final int timeoutMS = 5;
    private final int timeoutFrequency = 10;


    @Test
    public void testMultipleClientsCallingConcurrentlyAsyncBackend() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(callCount);
        ExecutorService clients = Executors.newFixedThreadPool(3);
        for ( int i=0; i < callCount; i++) {
            final int payload = i;
            clients.submit( new Runnable() {
                @Override
                public void run() {
                    assertSyncFacadeInvocation(payload, latch);
                }
            });
        }

        // just an approximative value -> might fail on slow machines
        int timeout = callCount / timeoutFrequency * timeoutMS * 5;
        latch.await(timeout, TimeUnit.MILLISECONDS );

        // assure we don't leak in error case
        Assert.assertEquals(0, rendevouzPlace.size());
    }

    private void assertSyncFacadeInvocation(final int requestPayload, CountDownLatch latch) {
        Request request = new Request(requestPayload);
        Response response = service.execute(request);
        Assert.assertEquals(Long.toString(request.l), response.s);
        LOGGER.info( "request=" + request.l + " / response="+ response.s );
        latch.countDown();
    }

    //------------------------------------------------------------------------------------------------------------------
    // infrastructure for sync service facade and backing async service execution
    //------------------------------------------------------------------------------------------------------------------

    class Response {
        String s;
        Response(String s) { this.s = s; }
    }

    class Request {
        String uid = UUID.randomUUID().toString();
        long l;
        Request(long l) { this.l = l; }
    }

    class RequestResponsePair {
        Request request;
        Response response;

        RequestResponsePair(Request request) {
            this.request = request;
        }

        RequestResponsePair(Request request, Response response) {
            this.request = request;
            this.response = response;
        }
    }

    /**
     * Sync service to the client who will block until it received a response.
     */
    class SyncClientServiceFacade {
        Logger clientLogger = Logger.getLogger(SyncClientServiceFacade.class);
        AsyncBackend backend = new AsyncBackend();

        public Response execute( Request request ) {
            final String key = request.uid;

            try {
                Exchanger<RequestResponsePair> exchanger = new Exchanger<RequestResponsePair>();

                rendevouzPlace.putIfAbsent(key, exchanger);
                backend.execute(request);
                RequestResponsePair responded =
                        exchanger.exchange(new RequestResponsePair(request), timeoutMS, TimeUnit.MILLISECONDS);
                String msg = "Clients = " + request.l;
                clientLogger.info(msg);
                rendevouzPlace.remove(key);
                return responded.response;

            } catch (Exception e){
                // make sure we remove entries which are expired or if something goes wrong
                rendevouzPlace.remove( key );
                LOGGER.error( "Could not finish request/response for = " + request.l );
                // signal it to sync client
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Some backend processing a given request and looks up the rendevouzPlace to hand over the response to appropriate
     * client thread.
     */
    class AsyncBackend {
        Logger backendLogger = Logger.getLogger(AsyncBackend.class);
        ExecutorService executorService = Executors.newFixedThreadPool(4);

        public void execute(final Request request) {
            executorService.submit( new Runnable() {
                @Override
                public void run() {
                    Exchanger<RequestResponsePair> exchanger = rendevouzPlace.get(request.uid);
                    long requestValue = request.l;
                    Response response = new Response(Long.toString(requestValue));
                    try {
                        String msg = "Async = " + response.s;
                        backendLogger.info(msg);

                        // simulate timeout
                        if ( requestValue % timeoutFrequency == 0) {
                            Thread.sleep( 2 * timeoutMS);
                        }

                        exchanger.exchange( new RequestResponsePair(request, response) );

                    } catch (InterruptedException e) { throw new RuntimeException( e ); }
                }
            });
        }
    }
}