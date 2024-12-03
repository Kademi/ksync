/*
 *       Copyright Kademi
 */
package co.kademi.sync;

/**
 *
 * @author dylan
 * @param <T>
 */
@FunctionalInterface
public interface CheckedConsumer<T> {

    void accept(T t) throws Exception;
}
