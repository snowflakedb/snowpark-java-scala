# 如何运行 UDAF 测试

## 前提条件

1. ✅ 本地 Snowflake regression environment 已启动并运行
2. ✅ 已编译项目: `sbt compile`
3. ✅ 已配置 `profile.properties` 文件

## 运行测试的方法

### 方法 1: 运行单个 UDAF 测试套件（推荐）

```bash
cd /home/gshe/snowpark-java-scala

# 只运行 UDAF 测试
sbt "testOnly *UDAFSuite"
```

### 方法 2: 运行特定的测试用例

```bash
# 运行特定测试（匹配名称）
sbt "testOnly *UDAFSuite -- -z anonymous"     # 运行包含 "anonymous" 的测试
sbt "testOnly *UDAFSuite -- -z temporary"     # 运行包含 "temporary" 的测试
sbt "testOnly *UDAFSuite -- -z permanent"     # 运行包含 "permanent" 的测试
sbt "testOnly *UDAFSuite -- -z multiple"      # 运行包含 "multiple" 的测试
```

### 方法 3: 在 SBT Console 中交互式运行

```bash
cd /home/gshe/snowpark-java-scala
sbt

# 在 SBT console 中
> testOnly *UDAFSuite
```

### 方法 4: 运行所有测试（包含 UDAF）

```bash
sbt test
```

⚠️ **注意**: 运行所有测试可能需要很长时间

## 快速验证步骤

### 1. 检查 Snowflake 连接

```bash
curl http://snowflake.reg.local:8082/healthcheck
# 或者你的环境 URL
```

### 2. 验证编译

```bash
cd /home/gshe/snowpark-java-scala
sbt compile
```

应该看到:
```
[success] Total time: ...
```

### 3. 运行单个测试

```bash
sbt "testOnly *UDAFSuite -- -z \"register temporary anonymous\""
```

## 预期输出

成功的测试输出应该类似：

```
[info] UDAFSuite:
[info] - UDAF - register temporary anonymous UDAF
[info] Anonymous UDAF Result: ...
[info] - UDAF - register temporary named UDAF
[info] Named UDAF Result: ...
[info] - UDAF - register permanent UDAF
[info] Permanent UDAF Result: ...
[info] - UDAF - multiple aggregations
[info] Multiple Aggregations: ...
[info] - UDAF - aggregate without groupBy
[info] Grand Total: 575.0
[info] Run completed in ... seconds.
[info] Total number of tests run: 5
[info] Suites: completed 1, aborted 0
[info] Tests: succeeded 5, failed 0, canceled 0, ignored 0, pending 0
[info] All tests passed.
[success] Total time: ...
```

## 测试内容说明

`UDAFSuite` 包含 5 个测试:

1. **register temporary anonymous UDAF** - 测试匿名临时 UDAF
2. **register temporary named UDAF** - 测试命名临时 UDAF  
3. **register permanent UDAF** - 测试永久 UDAF
4. **multiple aggregations** - 测试多个聚合
5. **aggregate without groupBy** - 测试不带分组的聚合

## 常见问题

### 问题 1: Connection refused

```
Error: Connection refused to snowflake.reg.local:8082
```

**解决方案**:
- 检查 Snowflake 服务是否启动
- 验证 `profile.properties` 中的 URL
- 确认 `/etc/hosts` 有正确配置

### 问题 2: 测试失败 - SQL 错误

```
SQL compilation error: UDAF not supported
```

**原因**: `createJavaUDAF` 方法中的 SQL 生成可能需要完善

**临时解决方案**: 
- 查看生成的 SQL (启用 DEBUG 日志)
- 根据 Snowflake 实际支持的语法调整

### 问题 3: 序列化错误

```
java.io.NotSerializableException
```

**解决方案**: 确保 State 类实现 `Serializable`

### 问题 4: 找不到测试

```
[info] No tests to execute.
```

**解决方案**:
```bash
# 重新编译测试代码
sbt clean test:compile
sbt "testOnly *UDAFSuite"
```

## 调试技巧

### 启用详细日志

```bash
# 方法 1: 环境变量
export LOG_LEVEL=DEBUG
sbt "testOnly *UDAFSuite"

# 方法 2: SBT 参数
sbt -Dlog.level=DEBUG "testOnly *UDAFSuite"
```

### 查看生成的代码

在 `UDXRegistrationHandler.scala` 的 `generateJavaUDAFCode` 方法中，有 `logDebug(code)`。
启用 DEBUG 日志后可以看到生成的 Java 代码。

### 单独测试序列化

在 SBT console 中:

```scala
import com.snowflake.snowpark._
import com.snowflake.snowpark.internal.JavaUtils
import com.snowflake.snowpark_java.udaf.JavaUDAF1

class TestSum extends JavaUDAF1[Double, Double, Double] {
  def initialize() = 0.0
  def accumulate(s: Double, v: Double) = if (v != null) s + v else s
  def merge(s1: Double, s2: Double) = s1 + s2
  def finish(s: Double) = s
}

val udaf = new TestSum()
val bytes = JavaUtils.serialize(udaf)
val restored = JavaUtils.deserialize(bytes)
println(s"Serialization works! Bytes: ${bytes.length}")
```

## 下一步

运行成功后：

1. ✅ 查看测试输出，确认结果正确
2. ✅ 检查生成的 SQL 语句
3. ✅ 完善 `createJavaUDAF` 方法
4. ✅ 添加更多复杂的测试用例
5. ✅ 集成到 CI/CD

## 一键运行脚本

```bash
#!/bin/bash
# 保存为 run_udaf_tests.sh

echo "=== Running UDAF Tests ==="
cd /home/gshe/snowpark-java-scala

echo "1. Checking Snowflake connection..."
curl -s http://snowflake.reg.local:8082/healthcheck || echo "Warning: Snowflake may not be running"

echo ""
echo "2. Compiling project..."
sbt compile

echo ""
echo "3. Running UDAF tests..."
sbt "testOnly *UDAFSuite"

echo ""
echo "=== Test Complete ==="
```

使用方法:
```bash
chmod +x run_udaf_tests.sh
./run_udaf_tests.sh
```

