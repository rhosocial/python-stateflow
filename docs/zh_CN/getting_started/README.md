# 快速开始

## 安装

```bash
pip install rhosocial-stateflow
```

本地开发：

```bash
cd python-stateflow
source .venv3.8/bin/activate
pip install -e '.[test]'
pytest
```

## 基础导入

```python
from rhosocial.stateflow import (
    OrderTemplate,
    OrderTemplateStep,
    SyncOrderFactory,
    SyncOrderDispatcher,
)
```
