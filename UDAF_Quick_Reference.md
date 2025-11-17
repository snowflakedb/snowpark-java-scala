# UDAF Quick Reference Card

## 📋 三种函数类型对比

```
UDF:  call(x) → y            单行 → 单值    (无状态)
UDTF: process(x) → [y1,y2]   单行 → 多行    (可选状态)
UDAF: agg([x1,x2,x3]) → y    多行 → 单值    (必需状态)
```

## ⚡ 快速上手

### 定义 UDAF

```java
class MyAvgUDAF implements JavaUDAF1<State, Double, Double> {
  static class State implements Serializable {
    double sum; long count;
    State(double s, long c) { sum=s; count=c; }
  }
  
  public State initialize() { return new State(0, 0); }
  
  public State accumulate(State s, Double v) {
    return v != null ? new State(s.sum+v, s.count+1) : s;
  }
  
  public State merge(State s1, State s2) {
    return new State(s1.sum+s2.sum, s1.count+s2.count);
  }
  
  public Double finish(State s) {
    return s.count > 0 ? s.sum / s.count : null;
  }
}
```

### 注册和使用

```java
// 注册
Column myAvg = session.udaf().registerTemporary("my_avg", new MyAvgUDAF());

// 使用
df.groupBy("category").agg(myAvg.apply(col("amount"))).show();
```

## 🔑 核心方法

| 方法 | 调用时机 | 作用 | 类比 |
|------|---------|------|------|
| `initialize()` | 每个分区开始 | 创建初始状态 | UDTF 构造函数 |
| `accumulate(state, value)` | 每一行 | 更新状态 | UDF `call()` |
| `merge(s1, s2)` | 分区合并 | 合并状态 | 特有（无类比） |
| `finish(state)` | 所有行处理完 | 生成结果 | UDTF `endPartition()` |

## 📐 接口选择

```java
// 0 参数 (如 COUNT(*))
JavaUDAF0<State, ReturnType>

// 1 参数 (如 SUM, AVG, MAX) ← 最常用
JavaUDAF1<State, Argument, ReturnType>

// 2 参数 (如 COVAR, WEIGHTED_AVG)
JavaUDAF2<State, Arg1, Arg2, ReturnType>
```

## 💡 常见模式

### Pattern 1: 简单聚合（State == Result）

```java
// SUM, MAX, MIN
class MySumUDAF implements JavaUDAF1<Double, Double, Double> {
  public Double initialize() { return 0.0; }
  public Double accumulate(Double s, Double v) { return s + v; }
  public Double merge(Double s1, Double s2) { return s1 + s2; }
  public Double finish(Double s) { return s; }  // 直接返回
}
```

### Pattern 2: 复合状态（State ≠ Result）

```java
// AVG (需要 sum 和 count)
class MyAvgUDAF implements JavaUDAF1<AvgState, Double, Double> {
  static class AvgState { double sum; long count; }
  
  public AvgState initialize() { return new AvgState(); }
  public AvgState accumulate(AvgState s, Double v) { /* 更新sum和count */ }
  public AvgState merge(AvgState s1, AvgState s2) { /* 合并sum和count */ }
  public Double finish(AvgState s) { return s.sum / s.count; }  // 计算平均
}
```

### Pattern 3: 集合状态

```java
// COUNT DISTINCT
class MyCountDistinctUDAF implements JavaUDAF1<Set<String>, String, Long> {
  public Set<String> initialize() { return new HashSet<>(); }
  public Set<String> accumulate(Set<String> s, String v) { s.add(v); return s; }
  public Set<String> merge(Set<String> s1, Set<String> s2) { s1.addAll(s2); return s1; }
  public Long finish(Set<String> s) { return (long) s.size(); }
}
```

## 🎯 使用场景

| 场景 | 使用类型 | 示例 |
|------|---------|------|
| 转换每一行 | UDF | `UPPER(name)`, `price * 1.1` |
| 拆分/展开行 | UDTF | `SPLIT_TO_TABLE(text)` |
| 统计聚合 | UDAF | `AVG(price)`, `SUM(amount)` |
| 自定义聚合 | UDAF | `MEDIAN()`, `PERCENTILE()` |
| 去重计数 | UDAF | `COUNT(DISTINCT x)` |
| 字符串拼接 | UDAF | `LISTAGG(name, ',')` |

## ⚠️ 关键注意事项

### 1. State 必须 Serializable

```java
// ✓ Good
static class State implements Serializable {
  double sum;
  long count;
}

// ✗ Bad - 会导致序列化失败
static class State {  // 缺少 Serializable!
  double sum;
}
```

### 2. 处理 NULL 值

```java
public State accumulate(State state, Double value) {
  if (value != null) {  // ← 总是检查 NULL
    return new State(state.sum + value, state.count + 1);
  }
  return state;
}
```

### 3. merge() 必须正确

```java
// merge() 的正确性对并行执行至关重要！

// ✓ Correct
public State merge(State s1, State s2) {
  return new State(s1.sum + s2.sum, s1.count + s2.count);
}

// ✗ Wrong - 只合并了 sum
public State merge(State s1, State s2) {
  return new State(s1.sum + s2.sum, s1.count);  // 忘记合并 count!
}
```

## 🔄 执行流程示例

```
数据: [10, 20, 30, 40] (4 rows)

Partition 1: [10, 20]     Partition 2: [30, 40]
─────────────────────     ─────────────────────
state = initialize()       state = initialize()
  = State(0, 0)             = State(0, 0)

accumulate(State(0,0), 10)   accumulate(State(0,0), 30)
  = State(10, 1)              = State(30, 1)

accumulate(State(10,1), 20)  accumulate(State(30,1), 40)
  = State(30, 2)              = State(70, 2)

         ↓                           ↓
    state1                       state2
         └──────── merge ──────────┘
                     ↓
           State(100, 4)
                     ↓
              finish(state)
                     ↓
                  25.0
```

## 📚 完整示例代码位置

```
JavaUDAFExamples.java       - 10 个示例实现
JavaUDAFExample.java        - 完整使用示例
UDF_UDTF_UDAF_Comparison.java - 三种类型对比
UDAF_Design.md              - 详细设计文档
```

## 🎓 学习顺序

1. **理解概念**: 读这个文件 (5 分钟)
2. **看简单例子**: `MySumUDAF`, `MyMaxUDAF` (10 分钟)
3. **复杂状态**: `MyAvgUDAF` (15 分钟)
4. **高级示例**: `MyStdDevUDAF`, `MyMedianUDAF` (20 分钟)
5. **实践**: 实现自己的 UDAF (30 分钟)

---

**Created by**: UDAF API Design Team  
**Based on**: UDF and UDTF implementation patterns  
**Status**: Design Complete ✅, Implementation TODO 🚧

