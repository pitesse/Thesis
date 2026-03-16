package com.polimi.f1.utils;

// functional interface for toCsvRow method references, must be serializable for flink
@FunctionalInterface
public interface SerializableToCsvRow<T> extends java.io.Serializable {

    String apply(T value);
}
