from unittest.mock import Mock

import pytest

from faust import Event, Record, Stream
from faust.joins import InnerJoin, Join, LeftJoin, OuterJoin, RightJoin


class User(Record):
    id: str
    name: str


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "join_cls,fields",
    [
        (Join, (User.id, User.name)),
        (InnerJoin, (User.id, User.name)),
        (LeftJoin, (User.id, User.name)),
        (OuterJoin, (User.id, User.name)),
        (RightJoin, (User.id, User.name)),
    ],
)
async def test_Join(join_cls, fields):
    stream = Mock(name="stream", autospec=Stream)
    j = join_cls(stream=stream, fields=fields)
    assert j.fields
    assert j.stream is stream

    with pytest.raises(NotImplementedError):
        await j.process(Mock(name="event", autospec=Event))


def test_eq_ne():
    stream = Mock(name="stream", autospec=Stream)
    assert InnerJoin(stream=stream, fields=(User.id,)) == InnerJoin(
        stream=stream, fields=(User.id,)
    )
    assert InnerJoin(stream=stream, fields=(User.id,)) != Join(
        stream=stream, fields=(User.id,)
    )
    assert InnerJoin(stream=stream, fields=(User.name,)) != InnerJoin(
        stream=stream, fields=(User.id,)
    )
