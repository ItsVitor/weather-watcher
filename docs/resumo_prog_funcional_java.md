# Mecanismos de Programação Funcional em Java

> Prof. João Paulo A. Almeida — Departamento de Informática, UFES

---

## 1. Motivação

Em linguagens como C, é possível passar funções como parâmetro usando **ponteiros para função**:

```c
void qsort(void *array, size_t count, size_t size,
           int (*comp)(const void *, const void *));
```

Em Java OO "clássico", a solução exigia uma **gambiarra**: criar uma classe concreta ou uma classe interna anônima que implementasse a interface desejada (ex.: `Comparator<T>`).

```java
// Opção 1: classe separada
class ComparadorTamanho implements Comparator<String> {
    public int compare(String s1, String s2) {
        return s1.length() - s2.length();
    }
}
Arrays.sort(vet, new ComparadorTamanho());

// Opção 2: classe interna anônima
Arrays.sort(vet, new Comparator<String>() {
    @Override
    public int compare(String s1, String s2) {
        return s1.length() - s2.length();
    }
});
```

---

## 2. Funções como *First-Class Citizens* (Java 8+)

A partir do Java 8, funções podem ser tratadas como valores, sem necessidade de workarounds. Isso viabiliza o uso de **higher-order functions** — funções que recebem outras funções como parâmetro.

Exemplos de higher-order functions: `Arrays.sort`, `Collections.sort`, e a **Stream API** (`filter`, `map`, `reduce`, `forEach`).

Os dois mecanismos principais são:

- **Expressão lambda** (função anônima): `(s1, s2) -> s1.length() - s2.length()`
- **Referência a método**: `String::length`

---

## 3. Interfaces Funcionais

Uma **interface funcional** é qualquer interface que possua **apenas um método abstrato**. Instâncias dela representam aquele método.

```java
@FunctionalInterface
public interface Comparator<T> {
    int compare(T o1, T o2);
}
```

A anotação `@FunctionalInterface` é opcional, mas ajuda o compilador a detectar erros (ex.: se houver mais de um método abstrato).

O Java consegue associar uma expressão lambda à interface correta porque ambas possuem a mesma assinatura de método.

---

## 4. Expressões Lambda

Funções anônimas com a sintaxe `(parâmetros) -> corpo`:

```java
(x) -> x + 1
(int x) -> x + 1
(x, y) -> x + y
(int x, int y) -> x + y

// Corpo com bloco
(x, y) -> {
    System.out.printf("%d + %d = %d\n", x, y, x + y);
}

// Sem parâmetros (compatível com Runnable)
() -> { System.out.println("I am a Runnable"); }
```

---

## 5. Tipos de Interfaces Funcionais (`java.util.function`)

O Java 8 fornece um conjunto rico de interfaces funcionais prontas:

| Interface | Assinatura do método | Entradas → Saída |
|---|---|---|
| `Runnable` | `void run()` | nenhuma → void |
| `Supplier<R>` | `R get()` | nenhuma → R |
| `Consumer<T>` | `void accept(T t)` | T → void |
| `Function<T,R>` | `R apply(T t)` | T → R |
| `BiFunction<T,U,R>` | `R apply(T t, U u)` | T, U → R |
| `Predicate<T>` | `boolean test(T t)` | T → boolean |
| `UnaryOperator<T>` | `T apply(T t)` | T → T |
| `BinaryOperator<T>` | `T apply(T t1, T t2)` | T, T → T |

Existem variantes especializadas para tipos primitivos (`IntSupplier`, `LongFunction`, `DoubleConsumer`, etc.) para evitar autoboxing.

---

## 6. Exemplos de Uso

### Predicate

```java
public static void printPersonsWithPredicate(
        List<Person> persons, Predicate<Person> tester) {
    for (Person p : persons) {
        if (tester.test(p)) p.printPerson();
    }
}

// Uso:
printPersonsWithPredicate(persons, p -> p.getAge() >= 18);

printPersonsWithPredicate(persons,
    p -> p.getGender() == Person.Sex.FEMALE
      && p.getAge() >= 18
      && p.getAge() <= 25);
```

### `Iterable.forEach`

```java
List<String> lista = Arrays.asList("Vitor", "Silva", "Souza");
lista.forEach(s -> System.out.print(s + " "));
```

### `List.sort`

```java
lista.sort((s1, s2) -> s1.compareTo(s2));
```

### Referência a método

```java
lista.forEach(System.out::println);
Arrays.sort(vet, Comparator.comparingInt(String::length));
```

---

## 7. Stream API

A **Stream API** (`java.util.stream`) representa uma sequência de objetos sobre a qual se aplicam operações encadeadas (pipeline), com suporte a paralelização.

Uma coleção é convertida em stream com `.stream()`.

### Operações principais

| Ação | Método | Interface funcional |
|---|---|---|
| Obter stream | `stream()` | — |
| Filtrar | `filter(predicate)` | `Predicate<T>` |
| Transformar | `map(function)` | `Function<T,R>` |
| Iterar | `forEach(consumer)` | `Consumer<T>` |
| Ordenar | `sorted(comparator)` | `Comparator<T>` |
| Coletar | `collect(collector)` | — |
| Verificar todos | `allMatch(predicate)` | `Predicate<T>` |
| Reduzir | `reduce(identity, accumulator)` | `BinaryOperator<T>` |

### Exemplo: antes vs. depois (processamento de transações)

**Antes (Java 7):**
```java
List<Transaction> groceryTransactions = new ArrayList<>();
for (Transaction t : transactions) {
    if (t.getType() == Transaction.GROCERY)
        groceryTransactions.add(t);
}
Collections.sort(groceryTransactions, new Comparator<Transaction>() {
    public int compare(Transaction t1, Transaction t2) {
        return t2.getValue().compareTo(t1.getValue());
    }
});
List<Integer> transactionIds = new ArrayList<>();
for (Transaction t : groceryTransactions)
    transactionsIds.add(t.getId());
```

**Depois (Java 8+):**
```java
List<Integer> transactionsIds =
    transactions.stream()
        .filter(t -> t.getType() == Transaction.GROCERY)
        .sorted(comparing(Transaction::getValue).reversed())
        .map(Transaction::getId)
        .collect(toList());
```

### Map/Reduce

```java
// Com valor inicial
int sum = list.stream()
    .map(Employee::getSalary)
    .reduce(0, (a, b) -> a + b);

// Sem valor inicial (retorna Optional)
int sum = list.stream()
    .map(Employee::getSalary)
    .reduce(Integer::sum);
```

### Criando mapas a partir de streams

```java
Map<String, String> isbnToTitle = books.stream()
    .collect(Collectors.toMap(Book::getIsbn, Book::getName));
```

### Agregações estatísticas

```java
DoubleSummaryStatistics stats = givenList.stream()
    .collect(summarizingDouble(String::length));

stats.getAverage();
stats.getCount();
stats.getMax();
stats.getMin();
stats.getSum();
```

---

## 8. Comparativo: Lambdas em C++

Em C++, lambdas usam a sintaxe `[capture](params) -> retorno { corpo }`:

```cpp
auto f = [](int x, int y) -> int { return x < y; };
```

O tipo é uma classe anônima que sobrecarrega `operator()`. Com `auto`, o tipo é inferido. É possível converter para ponteiro de função ou usar `std::function<void()>`.

### Capture (closures em C++)

| Sintaxe | Comportamento |
|---|---|
| `[]` | Sem captura |
| `[=]` | Captura tudo por cópia |
| `[&]` | Captura tudo por referência |
| `[x]` | Captura `x` por cópia |
| `[&x, y]` | `x` por referência, `y` por cópia |

### Algoritmos da STL

```cpp
// for_each
for_each(c.begin(), c.end(), [](int i){ cout << i << ' '; });

// transform (equivalente ao map)
transform(numbers.begin(), numbers.end(), doubled.begin(),
          [](int n) -> int { return n * 2; });

// accumulate (equivalente ao reduce)
int product = accumulate(v.begin(), v.end(), 1,
                         [](int a, int b) { return a * b; });
```

---

## Resumo Geral

```
Problema         → Solução Java 8+
─────────────────────────────────────────────────────────────
Passar função    → Expressão lambda ou referência a método
Tipagem          → Interface funcional (@FunctionalInterface)
Iterar coleção   → forEach (Consumer)
Filtrar          → stream().filter (Predicate)
Transformar      → stream().map (Function)
Agregar          → stream().reduce / collect
```
