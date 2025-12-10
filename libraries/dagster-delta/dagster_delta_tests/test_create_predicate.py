from dagster_delta._handler.utils.predicates import create_predicate


def test_create_predicate_string_list_proper_escaping():
    filters = [("country", "IN", ["foo", "bar", "ba'z", "ba''z"])]

    pred = create_predicate(filters)

    assert pred == "country IN ('foo', 'bar', 'ba''z', 'ba''''z')"
