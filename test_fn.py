import fn
import pytest


@pytest.mark.parametrize(
    "paths,obj,default_,expected",
    [
        [["spec", "tasks"], {"spec": {}}, {}, {}],
        [["spec", "tasks"], {"spec": {"tasks": None}}, None, None],
        [["spec", "tasks"], {"spec": {"tasks": None}}, {}, None],
    ],
)
def test_with_path(paths, obj, default_, expected) -> None:
    r = fn.with_path(*paths, default=default_)(obj)
    assert r == expected


@pytest.mark.parametrize(
    "val,condition,obj,expected",
    [
        [{"script": "echo hello"}, None, {}, {"script": "echo hello"}],
        [
            {"script": "echo hello"},
            None,
            {"name": "step-0"},
            {"name": "step-0", "script": "echo hello"},
        ],
        [{}, None, {"name": "step-0"}, {"name": "step-0"}],
        [
            {"script": "echo hello"},
            lambda obj: obj["name"].endswith("-0"),
            {"name": "step-0"},
            {"name": "step-0", "script": "echo hello"},
        ],
        [
            {"script": "echo hello"},
            lambda obj: int(obj["name"].split("-")[-1]) > 0,
            {"name": "step-0"},
            {"name": "step-0"},
        ],
    ],
)
def test_update(val, condition, obj, expected) -> None:
    r = fn.update(val, condition=condition)(obj)
    assert r == expected
