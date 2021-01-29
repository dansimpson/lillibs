package com.github.dansimpson.lilkv.util;

import java.util.Optional;

/**
 * An implementation of a Try monad, or something close enough and useful in practice.
 * 
 * @author Dan Simpson
 *
 * @param <T>
 */
public interface Try<T> {

	public static class Success<T> implements Try<T> {

		private final T object;

		public Success(T object) {
			this.object = object;
		}

		@Override
		public T get() {
			return object;
		}

		@Override
		public <K> Try<K> map(AttemptFn<T, K> fn) {
			try {
				return Try.success(fn.apply(object));
			} catch (Throwable t) {
				return Try.failure(t);
			}
		}

		@Override
		public boolean isFailure() {
			return false;
		}

		@Override
		public Throwable error() {
			return null;
		}

		@Override
		public <K> Try<K> flatMap(AttemptFn<T, Try<K>> fn) {
			try {
				return fn.apply(object);
			} catch (Throwable t) {
				return Try.failure(t);
			}
		}

		@Override
		public Try<T> tap(AttemptConsumer<T> handler) {
			try {
				handler.accept(object);
			} catch (Throwable t) {
				return Try.failure(t);
			}
			return this;
		}

		@Override
		public void throwIfFailure() throws Throwable {
		}
	}

	public static class Failure<T> implements Try<T> {

		private final Throwable error;

		public Failure(Throwable error) {
			this.error = error;
		}

		@Override
		public T get() {
			return null;
		}

		@Override
		public Throwable error() {
			return error;
		}

		@Override
		public <K> Try<K> map(AttemptFn<T, K> fn) {
			return Try.failure(error);
		}

		@Override
		public <K> Try<K> flatMap(AttemptFn<T, Try<K>> fn) {
			return Try.failure(error);
		}

		@Override
		public boolean isFailure() {
			return true;
		}

		@Override
		public void throwIfFailure() throws Throwable {
			throw error;
		}

		@Override
		public Try<T> tap(AttemptConsumer<T> handler) {
			return this;
		}

	}

	@FunctionalInterface
	public static interface AttemptConsumer<T> {

		public void accept(T item) throws Throwable;
	}

	@FunctionalInterface
	public static interface Attempt<T> {

		public T get() throws Throwable;
	}

	@FunctionalInterface
	public static interface AttemptFn<T, K> {

		public K apply(T value) throws Throwable;
	}

	public T get();

	public Throwable error();

	public Try<T> tap(AttemptConsumer<T> handler);

	public <K> Try<K> map(AttemptFn<T, K> fn);

	public <K> Try<K> flatMap(AttemptFn<T, Try<K>> fn);

	public boolean isFailure();

	public void throwIfFailure() throws Throwable;

	public default boolean isSuccess() {
		return !isFailure();
	}

	public default T orElse(T other) {
		if (isFailure()) {
			return other;
		}
		return get();
	}

	public default Optional<T> asOptional() {
		return Optional.ofNullable(get());
	}

	public static <T> Try<T> success(T object) {
		return new Success<T>(object);
	}

	public static <T> Try<T> failure(Throwable error) {
		return new Failure<T>(error);
	}

	public static <T> Try<T> presentOrFail(T object, Throwable failure) {
		return object == null ? failure(failure) : success(object);
	}

	public static <T> Try<T> attempt(Attempt<T> supplier) {
		try {
			T item = supplier.get();
			if (item == null) {
				return Try.failure(new Exception("Failed to get item from supplier!"));
			}
			return Try.success(item);
		} catch (Throwable t) {
			return Try.failure(t);
		}
	}

	public static <T> Try<T> attemptTry(Attempt<Try<T>> supplier) {
		try {
			return supplier.get();
		} catch (Throwable t) {
			return Try.failure(t);
		}
	}

}
