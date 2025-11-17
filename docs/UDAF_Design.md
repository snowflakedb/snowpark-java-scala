# Java/Scala UDAF API Design

## Overview

This document describes the design of the UDAF (User-Defined Aggregate Function) API for Snowpark, following the patterns established by UDF and UDTF.

## Function Type Comparison

| Feature | UDF | UDTF | UDAF |
|---------|-----|------|------|
| **Input Cardinality** | 1 row | 1 row | N rows |
| **Output Cardinality** | 1 value | N rows | 1 value |
| **State Management** | None | Optional (member vars) | Required (state object) |
| **Core Method** | `call(A1, A2, ...)` | `process(A1, A2, ...)` | `accumulate(State, A1, ...)` |
| **Finalization** | N/A | `endPartition()` | `finish(State)` |
| **Partition Support** | N/A | Yes | Yes (via `merge()`) |
| **Use Case** | Row transformation | Row expansion/flattening | Aggregation/reduction |

## Interface Hierarchy

```
Serializable
    │
    ├─── JavaUDF (stateless)
    │       ├─── JavaUDF0<RT>
    │       ├─── JavaUDF1<A1, RT>
    │       └─── JavaUDF2<A1, A2, RT>
    │
    ├─── JavaUDTF (optional state)
    │       ├─── JavaUDTF0
    │       ├─── JavaUDTF1<A1>
    │       └─── JavaUDTF2<A1, A2>
    │
    └─── JavaUDAF<S, RT> (required state)
            ├─── JavaUDAF0<S, RT>
            ├─── JavaUDAF1<S, A1, RT>
            └─── JavaUDAF2<S, A1, A2, RT>
```

## UDAF Lifecycle

```
┌─────────────────────────────────────────────────────────┐
│ Partition 1                  Partition 2                 │
│ ─────────────                ─────────────               │
│ state = initialize()         state = initialize()        │
│                                                          │
│ Row 1: state = accumulate()  Row 1: state = accumulate()│
│ Row 2: state = accumulate()  Row 2: state = accumulate()│
│ Row 3: state = accumulate()  Row 3: state = accumulate()│
│                                                          │
│      state1 ─────────┐      ┌───────── state2           │
└──────────────────────┼──────┼───────────────────────────┘
                       │      │
                       ↓      ↓
                ┌──────────────────┐
                │ merge(s1, s2)    │
                └────────┬─────────┘
                         ↓
                   finalState
                         ↓
                 ┌──────────────┐
                 │ finish(state)│
                 └──────┬───────┘
                        ↓
                    result
```

## Code Generation Strategy

Similar to UDTF, UDAF uses code generation to create a wrapper class:

### Input: User's UDAF Class

```java
class MyAvgUDAF implements JavaUDAF1<State, Double, Double> {
  // User's business logic
}
```

### Generated: Wrapper Class

```java
public class MyAvgUDAF_Wrapper {
  private MyAvgUDAF udaf;
  private MyAvgUDAF.State state;
  
  public MyAvgUDAF_Wrapper() {
    byte[] closure = { ... };  // Serialized UDAF
    this.udaf = (MyAvgUDAF) JavaUtils.deserialize(closure);
    this.state = udaf.initialize();
  }
  
  public void accumulate(Double value) {
    this.state = udaf.accumulate(this.state, value);
  }
  
  public void merge(byte[] otherStateBytes) {
    MyAvgUDAF.State otherState = deserialize(otherStateBytes);
    this.state = udaf.merge(this.state, otherState);
  }
  
  public Double finish() {
    return udaf.finish(this.state);
  }
  
  public byte[] serializeState() {
    return serialize(this.state);
  }
}
```

### Output: SQL Function

```sql
CREATE TEMPORARY AGGREGATE FUNCTION my_avg(value DOUBLE)
RETURNS DOUBLE
LANGUAGE JAVA
runtime_version = '17'
IMPORTS = ('@stage/snowpark.jar', '@stage/myudaf.jar')
HANDLER = 'MyAvgUDAF_Wrapper'
AS $$ 
  [Generated wrapper code]
$$
```

## Implementation Checklist

### Phase 1: Core Interfaces ✓
- [x] JavaUDAF.java (base interface)
- [x] JavaUDAF0.java
- [x] JavaUDAF1.java
- [x] JavaUDAF2.java
- [ ] JavaUDAF3.java to JavaUDAF22.java (auto-generated)
- [x] package-info.java

### Phase 2: Registration ✓
- [x] UDAFRegistration.scala (Scala API)
- [x] UDAFRegistration.java (Java wrapper)
- [ ] Add `udaf()` method to Session.java/scala
- [ ] Add `registerJavaUDAF` to JavaUtils.scala

### Phase 3: Code Generation (TODO)
- [ ] Add `generateJavaUDAFCode()` to UDXRegistrationHandler.scala
- [ ] Add `createJavaUDAF()` SQL generation
- [ ] Handle state serialization/deserialization
- [ ] Support for both inline and file-based closures

### Phase 4: Functions API (TODO)
- [ ] Add `callUDAF()` to Functions.java/scala
- [ ] Support for window functions (optional)

### Phase 5: Testing (TODO)
- [ ] JavaUDAFSuite.java (unit tests)
- [ ] Integration tests with real Snowflake
- [ ] Performance benchmarks

## Usage Examples

### Basic Usage

```java
// Define UDAF
class MyAvgUDAF implements JavaUDAF1<State, Double, Double> {
  // Implementation...
}

// Register
Column myAvg = session.udaf().registerTemporary("my_avg", new MyAvgUDAF());

// Use in aggregation
DataFrame df = session.table("sales");
df.groupBy("category")
  .agg(myAvg.apply(col("amount")).as("avg_amount"))
  .show();
```

### Advanced: Multiple UDAFs

```java
Column myAvg = session.udaf().registerTemporary("avg", new MyAvgUDAF());
Column myStdDev = session.udaf().registerTemporary("stddev", new MyStdDevUDAF());
Column myMedian = session.udaf().registerTemporary("median", new MyMedianUDAF());

df.groupBy("category").agg(
  myAvg.apply(col("price")).as("avg_price"),
  myStdDev.apply(col("price")).as("stddev_price"),
  myMedian.apply(col("price")).as("median_price")
).show();
```

### Permanent UDAF

```java
// Register once
session.udaf().registerPermanent("company_avg", new MyAvgUDAF(), "@udaf_stage");

// Use in any session
session.table("sales")
  .agg(callUDAF("company_avg", col("revenue")))
  .show();
```

## Comparison with Existing Patterns

### Pattern Consistency

All three function types follow the same registration pattern:

```java
// UDF
session.udf().registerTemporary("func", udfInstance);
Column result = callUDF("func", col("x"));

// UDTF  
session.udtf().registerTemporary("func", udtfInstance);
DataFrame result = session.tableFunction(tableFunc, col("x"));

// UDAF
session.udaf().registerTemporary("func", udafInstance);
Column result = callUDAF("func", col("x"));
```

### Code Reuse

| Component | UDF | UDTF | UDAF |
|-----------|-----|------|------|
| Serialization | ✓ | ✓ | ✓ |
| JAR Upload | ✓ | ✓ | ✓ |
| Dependency Management | ✓ | ✓ | ✓ |
| Code Generation Framework | ✓ | ✓ | ✓ |
| Type Inference | ✓ | ✓ | ✓ |

Estimated code reuse: **85-90%**

## Design Decisions

### 1. Why State as Parameter (not member variable)?

```java
// ❌ Bad: State as member (cannot merge)
class BadUDAF {
  private double sum = 0;
  void accumulate(Double v) { sum += v; }
  // Cannot merge states from different partitions!
}

// ✅ Good: State as parameter (enables merge)
class GoodUDAF implements JavaUDAF1<Double, Double, Double> {
  Double accumulate(Double state, Double v) { return state + v; }
  Double merge(Double s1, Double s2) { return s1 + s2; }  // Mergeable!
}
```

### 2. Why Three Core Methods?

- `initialize()`: Like UDTF constructor, but returns state object
- `accumulate()`: Like UDF's `call()`, but updates state
- `merge()`: Unique to UDAF, enables parallel aggregation
- `finish()`: Like UDTF's `endPartition()`, but returns single value

### 3. Type Parameters

- `<S>`: State type (can be simple like `Double` or complex like custom class)
- `<A1, A2, ...>`: Argument types (type-safe)
- `<RT>`: Return type (enforced at compile time)

## Implementation Notes

### State Serialization

State must be Serializable because:
1. Merging happens across JVM boundaries (different partitions)
2. State may be cached/spilled to disk
3. Fault tolerance requires state checkpointing

### Performance Considerations

1. **Immutable State**: Recommended for thread safety
2. **Efficient Merge**: O(1) merge is ideal (e.g., sum vs. list append)
3. **Memory**: State size should be bounded (avoid collecting all values)

### Best Practices

1. **Use JavaUDAF1** for 95% of cases (single argument aggregation)
2. **Make State Serializable** and ideally immutable
3. **Handle NULL values** explicitly in `accumulate()`
4. **Test merge()** thoroughly - it's critical for correctness
5. **Consider memory** - don't collect unbounded data in state

## Next Steps

1. Implement code generation in `UDXRegistrationHandler.scala`
2. Add SQL generation for `CREATE AGGREGATE FUNCTION`
3. Add `callUDAF()` to Functions API
4. Write comprehensive tests
5. Add documentation and examples

## See Also

- UDFRegistration.java - For scalar functions
- UDTFRegistration.java - For table functions
- JavaUDAFExamples.java - Example implementations

