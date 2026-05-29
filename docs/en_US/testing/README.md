# Testing

Run tests from the project directory:

```bash
cd python-stateflow
source .venv3.8/bin/activate
pytest
```

The MVP test suite covers:

- template validation;
- dependency propagation in the factory;
- dispatcher idempotency and downstream startup;
- sync/async API parity.
