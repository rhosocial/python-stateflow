# Getting Started

## Installation

```bash
pip install rhosocial-stateflow
```

For local development:

```bash
cd python-stateflow
source .venv3.8/bin/activate
pip install -e '.[test]'
pytest
```

## Basic import

```python
from rhosocial.stateflow import (
    OrderTemplate,
    OrderTemplateStep,
    SyncOrderFactory,
    SyncOrderDispatcher,
)
```
