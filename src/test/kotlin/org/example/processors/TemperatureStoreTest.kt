package org.example.processors

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TemperatureStoreTest {

    @Test
    fun `should add temperatures to store`() {
        val store = TemperatureStore(3)

        store.add(30.0)
        assertEquals(1, store.size())

        store.add(32.0)
        assertEquals(2, store.size())

        store.add(35.0)
        assertEquals(3, store.size())
    }

    @Test
    fun `should implement FIFO when capacity is reached`() {
        val store = TemperatureStore(3)

        store.add(30.0)
        store.add(32.0)
        store.add(35.0)
        assertEquals(listOf(30.0, 32.0, 35.0), store.getTemperatures())

        // Adicionar 4ª temperatura deve remover a primeira
        store.add(37.0)
        assertEquals(listOf(32.0, 35.0, 37.0), store.getTemperatures())
        assertEquals(3, store.size())
    }

    @Test
    fun `should check if store is full`() {
        val store = TemperatureStore(3)

        assertFalse(store.isFull())

        store.add(30.0)
        assertFalse(store.isFull())

        store.add(32.0)
        assertFalse(store.isFull())

        store.add(35.0)
        assertTrue(store.isFull())
    }

    @Test
    fun `should verify all temperatures above threshold`() {
        val store = TemperatureStore(3)

        store.add(36.0)
        store.add(37.0)
        store.add(38.0)

        assertTrue(store.allAboveThreshold(35.0))
        assertFalse(store.allAboveThreshold(37.0)) // 36.0 não está acima de 37.0
    }

    @Test
    fun `should return false when not all temperatures above threshold`() {
        val store = TemperatureStore(3)

        store.add(34.0) // Abaixo do limite
        store.add(36.0)
        store.add(37.0)

        assertFalse(store.allAboveThreshold(35.0))
    }

    @Test
    fun `should verify any temperature below threshold`() {
        val store = TemperatureStore(3)

        store.add(36.0)
        store.add(34.0) // Abaixo
        store.add(37.0)

        assertTrue(store.anyBelowThreshold(35.0))
    }

    @Test
    fun `should return false when all temperatures above threshold`() {
        val store = TemperatureStore(3)

        store.add(36.0)
        store.add(37.0)
        store.add(38.0)

        assertFalse(store.anyBelowThreshold(35.0))
    }

    @Test
    fun `should clear all temperatures`() {
        val store = TemperatureStore(3)

        store.add(30.0)
        store.add(32.0)
        store.add(35.0)

        assertEquals(3, store.size())

        store.clear()

        assertEquals(0, store.size())
        assertFalse(store.isFull())
    }

    @Test
    fun `should return empty list when store is empty`() {
        val store = TemperatureStore(3)

        assertTrue(store.getTemperatures().isEmpty())
        assertFalse(store.allAboveThreshold(35.0))
    }

    @Test
    fun `should handle capacity of 1`() {
        val store = TemperatureStore(1)

        store.add(30.0)
        assertTrue(store.isFull())
        assertEquals(listOf(30.0), store.getTemperatures())

        store.add(35.0)
        assertTrue(store.isFull())
        assertEquals(listOf(35.0), store.getTemperatures())
    }
}
