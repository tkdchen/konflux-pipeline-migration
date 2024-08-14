import copy
import re


def _(*args):
    return args


def apply(*fns):
    def _apply(obj):
        for fn in fns:
            obj = fn(obj)
        return obj

    return _apply


def with_path(*parts, default=None):
    def _with_path(obj):
        for part in parts:
            if part not in obj and default is not None:
                obj[part] = default
            obj = obj[part]
        return obj

    return _with_path


def if_matches(fn):
    def _if_matches(obj):
        return [o for o in obj if fn(o)]
    return _if_matches


def nth(idx):
    def _nth(obj):
        if len(obj) > idx:
            return obj[idx]
        else:
            raise IndexError(f'Given list with {len(obj)} elements, and tried to index element at {idx}')
    return _nth


def for_each(*fns):
    def _for_each(obj):
        for i, o in enumerate(obj):
            for fn in fns:
                obj[i] = fn(o)
        return obj
    return _for_each


def task_ref_matches(name, bundle):
    def _task_ref_matches(task):
        name_match = False
        bundle_match = False
        kind_match = False

        name_re = re.compile(name)
        bundle_re = re.compile(bundle)
        for p in task.get('taskRef', {}).get('params', []):
            value = p.get('value')
            match p.get('name', ''):
                case 'kind':
                    kind_match = value == 'task'
                case 'name':
                    name_match = name_re.match(value)
                case 'bundle':
                    bundle_match = bundle_re.match(value)
            if name_match and bundle_match and kind_match:
                return True
        return False
    return _task_ref_matches


def delete_if(fn):
    def _delete_if(obj):
        idxs = []
        for i, o in enumerate(obj):
            if fn(o):
                idxs.append(i)
        offset = 0
        for i in idxs:
            obj.pop(i - offset)
            offset += 1
        return obj
    return _delete_if


def delete_key(key):
    def _delete_key(obj):
        obj.pop(key, None)
        return obj
    return _delete_key


def append(to_add):
    def _add(obj):
        obj.append(copy.deepcopy(to_add))
        return obj
    return _add


def update(val, condition=None):
    def _update(obj):
        if condition is None or condition(obj):
            obj.update(val)
        return obj
    return _update
