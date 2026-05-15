package org.example.processors

/**
 * State Store para manter histórico de temperaturas em uma janela temporal.
 * Implementado como fila circular (FIFO) com capacidade fixa.
 */
class TemperatureStore(private val capacity: Int) {
    private val temperatures = mutableListOf<Double>()

    /**
     * Adiciona uma temperatura ao store.
     * Se a capacidade for atingida, remove a temperatura mais antiga (FIFO).
     */
    fun add(temperature: Double) {
        if (temperatures.size >= capacity) {
            temperatures.removeAt(0) // Remove o mais antigo
        }
        temperatures.add(temperature)
    }

    /**
     * Verifica se o store está cheio (atingiu a capacidade).
     */
    fun isFull(): Boolean {
        return temperatures.size == capacity
    }

    /**
     * Verifica se todas as temperaturas no store estão acima do limite.
     */
    fun allAboveThreshold(threshold: Double): Boolean {
        if (temperatures.isEmpty()) return false
        return temperatures.all { it > threshold }
    }

    /**
     * Retorna o tamanho atual do store.
     */
    fun size(): Int {
        return temperatures.size
    }

    /**
     * Limpa todas as temperaturas do store.
     */
    fun clear() {
        temperatures.clear()
    }

    /**
     * Retorna uma cópia das temperaturas armazenadas.
     */
    fun getTemperatures(): List<Double> {
        return temperatures.toList()
    }

    /**
     * Verifica se alguma temperatura está abaixo do limite.
     */
    fun anyBelowThreshold(threshold: Double): Boolean {
        return temperatures.any { it <= threshold }
    }
}
