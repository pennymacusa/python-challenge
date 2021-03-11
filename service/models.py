"""Service models and factories."""
import re
import logging
from copy import copy
from typing import Generator, List, Any


# Logging setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Model objects
class JSONManifest:
    """JSONManifest object.

    This objects as a container for a json document. JSONManifest instances,
    initialized with a python dictionary and a list of rules, will act as an
    iterator for the resulting combination of the two documents.

    The logic is simple: for every rule from the passed-in list of rules,
    if the `source` values matches the path for a value in the data, then
    the manifest will output the `target` along with the value.

    Parameters
    ----------
    data : dict{str:any}
        The data dictionary that the JSONManifest instance will wrap.
    rules : list[dict]
        A list of rules to apply to the ingested data.
        Rules must be a list of dictionaries, each with a `source` and `target`
        key to operate correctly.

    Attributes
    ----------
    data : dict{str:any}
        The ingested data.
    rules : list[dict]
        The ingested rules.
    items : dict{str:any}
        A dictionary where the keys are the target paths and the values are
        the values, after the transformation has occurred.

    """

    # Instance attributes
    @property
    def data(self) -> dict:
        """Return a copy of the internal read-only _data attributes."""
        return copy(self._data)

    @property
    def rules(self) -> list:
        """Return a copy of the internal read-only _rules attribute."""
        return copy(self._rules)

    @property
    def items(self) -> list:
        """Return a dictionary of the mapped data, per the given rules."""
        return dict(iter(self))

    def __init__(self, data: dict = None, rules: list = None):
        data = {} if data is None else data
        rules = [] if rules is None else rules
        self._data, self._rules = data, rules

        # Flatten source data for faster parsing
        self._fdata = dict(self.flatten(self._data))

    def __iter__(self):
        """Iterate on the rules and items, yielding only those which match."""
        for rule in self._rules:
            for path, value in self._fdata.items():
                if rule.get('source') == path:
                    yield rule.get('target'), value

    # Static methods
    @staticmethod
    def flatten(data: dict) -> Generator:
        """Flatten the given dictionary to a list of paths and values.

        Parameters
        ----------
        data : dict{str:any}
            The data dictionary which should be flattened.

        Returns
        -------
        Generator
            Returns a generator, which when iterated on, will yield key-value
            pairs where the values are the individual values from the ingested
            data and the keys are the valid JSONPaths to those values.

        """

        def iter_child(cdata: Any, keys: List[str] = None):
            keys = [] if keys is None else keys

            if isinstance(cdata, dict):
                for key, value in cdata.items():
                    yield from iter_child(value, keys + [key])

            elif isinstance(cdata, list):
                for idx, value in enumerate(cdata):
                    key = f'{keys[-1]}[{str(idx)}]'
                    yield from iter_child(value, keys[:-1] + [key])

            else:
                yield '.'.join(keys), cdata

        yield from iter_child(data, ['$'])


# Factory objects
class JSONFactory:
    """JSONFactory object.

    This class acts as a factory ontop of JSONManifest objects to
    reconstitute all mapped values back into a valid JSON document, which is
    called the "Projection" or the "Projected JSON".

    Parameters
    ----------
    manifest : JSONManifest
        The JSONManifest object, which is a container for the rules and data
        that should be combined to create the projected JSON.

    Attributes
    ----------
    RE_PAT : re.Pattern
        A regex pattern which parses JSONPaths for queries.
    RE_IDX : dict{str:str}
        A dictionary which represents the group names of `RE_PAT`.

    """

    # Class attributes
    _vals = r"(?:['\"]\s*[\w\.\s-]+\s*['\"]|\d+|true|false|null)"
    _query = fr"(?:@\.\w+\s*==\s*{_vals})"

    _stmt_index = r"(?P<index>\d+)"
    _stmt_query = fr"(?P<query>\?\({_query}(?:\s*&&\s*{_query})*\))"

    RE_PAT = re.compile(
        fr"\.(?P<key>\w+)(?:\[(?:{_stmt_index}|{_stmt_query})\])*"
    )  # Super nasty regex pattern, so split into smaller patterns
    RE_IDX = RE_PAT.groupindex

    # Class methods
    @classmethod
    def parse_path(cls, path):
        """Parse paths, indices, and queries from a valid JSONPath.

        Parameters
        ----------
        path : str
            The valid JSONPath to parse out keys, indicies, and queries from.

        Returns
        -------
        dict{str:str}
            Returns a dictionary where the keys and values are the group names
            and the result, if any otherwise None, found from the regex
            operation.

        """
        matches = []
        for match in cls.RE_PAT.findall(path):
            match = [_ if _ != '' else None for _ in match]
            matches.append(dict(zip(cls.RE_IDX, match)))
        return matches

    @classmethod
    def insert_value(cls, path, value, record=None):
        """Insert a value at a specfied path into the given record.

        Parameters
        ----------
        path : str
            The path to insert the value at.
        value : any
            The value to insert.
        record : dict{str:any}
            The record to insert the value into.

        Returns
        -------
        dict{str:any}
            Returns the updated record.

        """
        record = {} if record is None else record

        def _get_index(key):
            matches = re.search(r"\[(?P<index>\d+)\]", key)
            if matches:
                return (
                    key.replace(matches.group(), ''),
                    int(matches.group('index')),
                )
            return None, None

        def _iter(keys=None, reference=None):
            keys = [] if keys is None else keys
            reference = {} if reference is None else reference

            if not keys:
                return

            key = keys.pop(0)
            index = _get_index(key)

            if index:
                key, idx = index
                if not key in reference:
                    reference[key] = []

                rlen = len(reference[key])
                if rlen <= idx:
                    for _ in range(idx + 1 - rlen):
                        reference[key].append({})

                ref = reference[key][idx]
                reference[key][idx] = _iter(keys, ref) if keys else value

            else:
                ref = reference.get(key, {})
                reference[key] = _iter(keys, ref) if keys else value

            return reference

        path_keys = path.split('.')
        if path_keys[0] == '$':
            path_keys.pop(0)

        record = _iter(path_keys, record)
        return record

    @classmethod
    def insert_query(cls, path, value, record=None):
        """Insert a value at a specfied path into the given record.

        This method is very similar to insert_value except it assumes the
        path includes a query. This method will then perform very similarly
        to insert_value, except it will ensure that the query is met when
        the value is inserted.

        Parameters
        ----------
        path : str
            The path to insert the value at.
        value : any
            The value to insert.
        record : dict{str:any}
            The record to insert the value into.

        Returns
        -------
        dict{str:any}
            Returns the updated record.

        """
        record = {} if record is None else record

        def _iter(keys=None, reference=None):
            keys = [] if keys is None else keys
            reference = {} if reference is None else reference
            key, index, query = keys.pop(0).values()

            # convert index to integer, if exists
            if index is not None:
                index = int(index)

            # 4 possible cases:
            #    (a) query w/ index :     process query and update only that index from result
            #    (b) query w/o index: :   process query and update all values
            #    (c) just index :         grab just that index
            #    (d) only a key given :   treat like a dict key and update that value

            if query is not None:
                conditions = [
                    tuple(
                        t.strip()
                        .replace('@.', '')
                        .replace('\'', '')
                        .replace('"', '')
                        .strip()
                        for t in s.strip().split('==')
                    )
                    for s in query[2:-1].split('&&')
                ]

                if not key in reference:
                    reference[key] = []

                indices = []
                for i, ele in enumerate(reference[key]):
                    if all(ele.get(k) == v for k, v in conditions):
                        indices.append(i)

                if index is not None:
                    rlen = len(indices)
                    if rlen <= index:
                        for _ in range(index + 1 - rlen):
                            reference[key].append(dict(conditions))
                        indices.append(-1)
                        index = -1

                    ref = reference[key][indices[index]]
                    reference[key][indices[index]] = (
                        _iter(keys, ref) if keys else value
                    )
                else:
                    if not indices:
                        reference[key].append(dict(conditions))
                        indices.append(-1)

                    for idx in indices:
                        ref = reference[key][idx]
                        reference[key][idx] = (
                            _iter(list(keys), ref) if keys else value
                        )

            elif index is not None:
                if not key in reference:
                    reference[key] = []

                rlen = len(reference[key])
                if rlen <= index:
                    for _ in range(index + 1 - rlen):
                        reference[key].append(
                            {}
                        )  # Change to type of child element

                ref = reference[key][index]
                reference[key][index] = _iter(keys, ref) if keys else value

            else:
                ref = reference.get(key, {})
                reference[key] = _iter(keys, ref) if keys else value

            return reference

        path_keys = cls.parse_path(path)
        record = _iter(path_keys, record)

        return record

    # Instance attributes
    def __init__(self, manifest: JSONManifest):
        self._manifest = manifest

    # Instance methods
    def get_projection(self):
        """Generate the projection for the given manifest.

        Returns
        -------
        dict{str:any}
            Returns the generated projected json for the given manifest.

        """
        queries, record = [], {}
        for path, value in self._manifest:

            # Prioritize non-queries before queries
            if '?' in path:
                queries.append((path, value))
                continue

            self.insert_value(path, value, record)

        for path, value in queries:
            self.insert_query(path, value, record)

        return record
