import asyncio
import pytest
import pytest_asyncio
from dataclasses import dataclass
from typing import (
    List,
    Dict,
    Optional
)

from deta import Deta, Record
from deta.base import Base
import os

try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass


@dataclass
class SDKKeys:
    project_key: str
    db_name: str

@pytest.fixture
def test_items():
    items = [
        {"key": "existing1", "value": "test"},
        {"key": "existing2", "value": 7},
        {"key": "existing3", "value": 44},
        {"key": "existing4", "value": {"name": "patrick"}},
        # TODO not passing the tests, library can't delete it
        # {"key": "%@#//#!#)#$_", "value": 0, "list": ["a"]},
    ]
    return items

@pytest.fixture
def sdk_keys():
    key = os.getenv("DETA_TEST_PROJECT_KEY")
    name = os.getenv("DETA_TEST_BASE_NAME")
    assert key is not None, "set `DETA_TEST_PROJECT_KEY` to env variable"
    assert name is not None, "set `DETA_TEST_BASE_NAME` to env variable"
    return SDKKeys(key, name)

@pytest_asyncio.fixture
async def base(sdk_keys):
    async with Deta(sdk_keys.project_key) as d:
        base = d.base(sdk_keys.db_name)
        yield base

async def clear_base(base: Base, size: Optional[int] = None):
    '''
    Clear base and assert number of clearing items if need
    '''
    items = await base.get()

    if size is not None:
        assert len(items) == size,\
            f"The base has more items than expected\nItems:\n{items}"

    for item in items:
        key = item['key']
        print('Start deleting key:', key)
        await base.delete(key)
    items = await base.get()
    assert len(items) == 0

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_get_all_empty_db_2(base):
    '''
    get all items from empty db
    '''
    # get all items
    items = await base.get()
    assert type(items) == list
    assert len(items) == 0

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_put_one_item(base: Base):
    '''
    put one item to db
    '''
    item = {
        'name': 'Evgenii',
        'surname': 'Kolodin',
        'age': 28,
        'city': 'Manila'
    }
    resp = await base.put(Record(item, key='qwerty'))
    # check all the response structure
    assert type(resp) == dict
    assert 'processed' in resp
    assert type(resp['processed']) == dict
    assert 'items' in resp['processed']
    resp_items = resp['processed']['items']
    assert type(resp_items) == list
    # check items
    assert len(resp_items) == 1
    resp_item = resp_items[0]
    assert resp_item == item
    
    await clear_base(base)

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_put_items(base: Base, test_items):
    '''
    put several items to db
    '''
    records = [
        Record(
            {x: item[x] for x in item if x != 'key'},
            key=item['key']
        )
    for item in test_items]
    resp = await base.put(*records)

    resp_items = resp['processed']['items']
    # check items
    assert len(resp_items) == len(test_items)
    for item in test_items:
        assert item in resp_items

    await clear_base(base, len(resp_items))

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_get_item_exists(base: Base, test_items):
    '''
    get one existing item
    '''
    # creating records from test items dict
    records = [
        Record(
            {x: item[x] for x in item if x != 'key'},
            key=item['key']
        )
    for item in test_items]
    # put records to database
    await base.put(*records)
    # get one existing item
    resp_item_list = await base.get('existing3')
    assert type(resp_item_list) == list
    assert len(resp_item_list) == 1
    resp_item = resp_item_list[0]
    # print('base.get(existing3):', resp_item)
    assert resp_item == test_items[2]

    await clear_base(base, len(test_items))

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_get_items_exist(base: Base, test_items):
    '''
    get several existing item
    '''
    # creating records from test items dict
    records = [
        Record(
            {x: item[x] for x in item if x != 'key'},
            key=item['key']
        )
    for item in test_items]
    # put records to database
    await base.put(*records)
    # get one existing item
    resp_item_list = await base.get('existing3', 'existing1')
    assert type(resp_item_list) == list
    assert len(resp_item_list) == 2
    
    assert test_items[0] in resp_item_list
    assert test_items[2] in resp_item_list

    await clear_base(base, len(test_items))

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_get_item_not_exists(base: Base, test_items):
    '''
    get one not existing item
    '''
    # creating records from test items dict
    records = [
        Record(
            {x: item[x] for x in item if x != 'key'},
            key=item['key']
        )
    for item in test_items]
    # put records to database
    await base.put(*records)
    # get one NOT existing item
    resp_item_list = await base.get('key_is_not_exists')
    assert resp_item_list is None

    await clear_base(base, len(test_items))

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_get_items_not_exists(base: Base, test_items):
    '''
    several items aren't exist
    '''
    # creating records from test items dict
    records = [
        Record(
            {x: item[x] for x in item if x != 'key'},
            key=item['key']
        )
    for item in test_items]
    # put records to database
    await base.put(*records)
    # get one NOT existing item
    resp_item_list = await base.get(
        'key_is_not_exists_1',
        'existing2',
        'key_is_not_exists_2'
    )
    assert type(resp_item_list) == list
    assert len(resp_item_list) == 1
    assert resp_item_list[0] == test_items[1]

    await clear_base(base, len(test_items))

@pytest.mark.asyncio
@pytest.mark.filterwarnings("ignore::UserWarning")
async def test_delete(base: Base, test_items):
    '''
    delete item
    '''
    # creating records from test items dict
    records = [
        Record(
            {x: item[x] for x in item if x != 'key'},
            key=item['key']
        )
    for item in test_items]
    # put records to database
    await base.put(*records)
    # delete one existing item
    resp = await base.delete(
        'existing2',
    )
    # the result is None
    assert resp is None
    # delete one not existing item
    resp = await base.delete(
        'key_not_exists',
    )
    # the result is None
    assert resp is None
    # check that only len(test_items) items remain in db
    items_in_db = await base.get()
    assert type(items_in_db) == list
    assert len(items_in_db) == len(test_items) - 1
    for item in test_items:
        if item['key'] != 'existing2':
            assert item in items_in_db

    await clear_base(base, len(test_items))
