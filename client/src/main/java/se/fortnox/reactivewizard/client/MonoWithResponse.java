package se.fortnox.reactivewizard.client;

import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClientResponse;
import reactor.util.annotation.NonNull;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Internal class to help support getting the full response as return value when Mono is returned
 * @param <T> the type of data to be returned
 */
class MonoWithResponse<T> extends Mono<T> {

    private final Mono<T>                             inner;
    private final AtomicReference<HttpClientResponse> httpClientResponse;

    MonoWithResponse(Mono<T> inner, AtomicReference<HttpClientResponse> httpClientResponse) {
        this.inner = inner;
        this.httpClientResponse = httpClientResponse;
    }

    HttpClientResponse getResponse() {
        if (httpClientResponse.get() == null) {
            throw new IllegalStateException("This method can only be called after the response has been received");
        }
        return httpClientResponse.get();
    }

    static <T> MonoWithResponse<T> from(MonoWithResponse<T> monoWithResponse, Mono<T> inner) {
        return new MonoWithResponse<T>(inner, monoWithResponse.httpClientResponse);
    }

    @Override
    public void subscribe(@NonNull CoreSubscriber<? super T> actual) {
        inner.subscribe(actual);
    }
}
