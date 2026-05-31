import uuid

from rhosocial.stateflow import OrderSubProcess, SubProcessDependency


def make_subprocess():
    return OrderSubProcess(
        process_id=uuid.uuid4(),
        step_name="step",
        handler_class="tests.Handler",
        terminal_states=["done"],
        advance_states=["done"],
    )


def test_for_subprocess_builds_dependency_edge():
    process_id = uuid.uuid4()
    subprocess = make_subprocess()
    upstream = make_subprocess()

    dependency = SubProcessDependency.for_subprocess(process_id, subprocess, upstream)

    assert dependency.process_id == process_id
    assert dependency.subprocess_id == subprocess.id
    assert dependency.depends_on_id == upstream.id


def test_group_by_subprocess_groups_edges_by_downstream_id():
    first = make_subprocess()
    second = make_subprocess()
    upstream = make_subprocess()
    dependencies = [
        SubProcessDependency.for_subprocess(first.process_id, first, upstream),
        SubProcessDependency.for_subprocess(second.process_id, second, upstream),
    ]

    grouped = SubProcessDependency.group_by_subprocess(dependencies)

    assert grouped[first.id] == [dependencies[0]]
    assert grouped[second.id] == [dependencies[1]]
