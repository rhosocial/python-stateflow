# 测试

在项目目录下运行测试：

```bash
cd python-stateflow
source .venv3.8/bin/activate
pytest
```

MVP 测试覆盖：

- 模板校验；
- 工厂中的依赖传播；
- 调度器幂等与下游启动；
- 同步/异步 API 对等。
