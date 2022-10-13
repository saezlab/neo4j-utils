#!/usr/bin/env python

import random
import string
import math
import itertools
import collections

import tqdm

import neo4j_utils


def rstring(l = 10, lower = False, title = False, upper = False):
    """
    Create a random string of a given length and capitalization.
    """

    charclass = 'lowercase' if lower else 'uppercase' if upper else 'letters'

    chars = getattr(string, f'ascii_{charclass}')

    result = ''.join(random.sample(chars, l))

    result = result.capitalize() if title else result

    return result


def unique_labels(n, **kwargs):
    """
    Create a set with the desired number of unique random strings.
    """

    result = set()

    for i in range(int(1e4)):

        result.update({rstring(**kwargs) for _ in range(int(1e4))})

        if len(result) >= n:

            break

    return list(result)[:int(n)]


def random_nodes(n = 1e5, nlabels = 1, nprops = 0) -> list[dict[str, str]]:
    """
    A list of dicts, each dict represents a node, consisting of an `ID`
    and a `label`.

    Args:
        nprops:
            Number of node properties.
    """

    labels = {
        rstring(l = 10, lower = True, title = True)
        for _ in range(nlabels)
    }

    result = []

    props = [
        k_ids(n, nlabels)
        for propid in range(nprops + 1)
    ]

    prop_names = [f'prop{i}' for i in range(nprops)]

    for label, *_props in zip(labels, *props):

        result.extend(
            dict(
                label = label,
                ID = _prop[0],
                **dict(zip(prop_names, _prop[1:]))
            )
            for _prop in zip(*_props)
        )

    return result[:int(n)]


def random_rels(
        nrels = 2e6,
        nnodes = 1e6,
        ntypes = 1,
        nlabels = 1,
        nnprops = 0,
        nrprops = 0,
    ):
    """
    A list of dicts, each represents a relation with the labels and IDs of
    the source and target nodes, the ID and type of the relation.
    """

    types = [
        rstring(l = 5).upper()
        for _ in range(ntypes)
    ]

    nodes = (
        nnodes
            if isinstance(nnodes, list) else
        random_nodes(nnodes, nlabels = nlabels, nprops = nnprops)
    )

    edges = random_nodes(nrels, nlabels = ntypes, nprops = nrprops)

    result = []
    counts = [10] * len(nodes)

    result = [
        dict(
            source_label = anode['label'],
            target_label = bnode['label'],
            source_id = anode['ID'],
            target_id = bnode['ID'],
            ID = edge['ID'],
            rel_type = edge['label'],
            **dict(i for i in edge.items() if i[0].startswith('prop'))
        )
        for edge, anode, bnode in zip(
            edges,
            random.sample(nodes, len(edges), counts = counts),
            random.sample(nodes, len(edges), counts = counts),
        )
    ]

    return result, nodes


def k_ids(total, k = 1, **kwargs) -> list[list[str]]:
    """
    `k` lists with `total / k` unique random strings in each. In other words,
    `total` number of unique random strings divided into `k` equal size
    batches.
    """

    ids = unique_labels(n = total, **kwargs)

    chunk_size = math.ceil(total / k)

    return list(chunks(ids, chunk_size, pbar = False))


def chunks(lst, n, pbar = True):
    """
    Yields successive `n`-sized chunks from `lst`.
    """

    it = range(0, len(lst), int(n))
    it = tqdm.tqdm(it) if pbar else it

    for i in it:

        yield lst[i:i + int(n)]


def by_label(nodes):
    """
    Sorts nodes by their labels.
    """

    return _group_by(nodes, 'label')


def by_type(rels):
    """
    Sorts relations by their types and the types of the endpoints.
    """

    return _group_by(rels, 'rel_type', 'source_label', 'target_label')


def _group_by(items, *keys):

    result = collections.defaultdict(list)

    keys = sorted(keys)

    for it in items:

        key = tuple(it[k] for k in keys)
        key = key[0] if len(key) == 1 else key

        result[key].append(it)

    return dict(result)


def insert0(driver, data = None, batch_size = 1.5e4, **kwargs):

    data = data or random_rels(**kwargs)

    for batch in chunks(data, batch_size):

        driver.query(
            """
            UNWIND $entities AS ent
            MERGE (s:Anything {ID: ent.source_id})
            MERGE (t:Anything {ID: ent.target_id})
            MERGE (s)-[rel:Rel]->(t)
            set rel.ID = ent.ID
            """,
            parameters = {'entities': batch},
        )


def insert1(driver, data = None, batch_size = 1.5e4, **kwargs):

    data = data or random_rels(**kwargs)

    nodes = list(sorted(
        {d['source_id'] for d in data} |
        {d['target_id'] for d in data}
    ))

    driver.query('CREATE INDEX node_id IF NOT EXISTS FOR (n:Anything) ON (n.ID)')
    driver.query('CREATE LOOKUP INDEX node_la IF NOT EXISTS FOR (n) ON EACH labels(n)')
    # this is slower:
    # driver.query('CREATE CONSTRAINT node_cs IF NOT EXISTS ON (n:Anything) ASSERT n.ID IS UNIQUE;')
    driver.query('CREATE INDEX rel_id IF NOT EXISTS FOR ()-[r:Rel]->() ON (r.ID)')
    driver.query('CREATE LOOKUP INDEX rel_ty IF NOT EXISTS FOR ()-[r]->() ON type(r)')
    driver.query('CALL db.awaitIndexes()')

    for batch in chunks(nodes, batch_size):

        batch = [{'node_id': x} for x in batch]

        driver.query(
            """
            UNWIND $entities AS ent
            MERGE (n:Anything {ID: ent.node_id})
            """,
            parameters = {'entities': batch},
        )

    print('Creating node text index.')
    driver.query('CREATE TEXT INDEX node_it IF NOT EXISTS FOR (n:Anything) ON (n.ID)')
    driver.query('CALL db.awaitIndexes()')

    data = list(sorted(
        data,
        key = lambda r: (r['source_id'], r['target_id'])
    ))

    for batch in chunks(data, batch_size):

        driver.query(
            """
            UNWIND $entities AS ent
            MATCH (s:Anything {ID: ent.source_id})
            MATCH (t:Anything {ID: ent.target_id})
            MERGE (s)-[rel:Rel]->(t)
            set rel.ID = ent.ID
            """,
            parameters = {'entities': batch},
        )

    print('Creating rel text index.')
    driver.query('CREATE TEXT INDEX rel_it IF NOT EXISTS FOR ()-[r:Rel]->() ON (r.ID)')
    driver.query('CALL db.awaitIndexes()')


def insert2(driver, data = None, batch_size = 1.5e4, **kwargs):

    data = data or random_rels(**kwargs)

    nodes = list(
        {d['source_id'] for d in data} |
        {d['target_id'] for d in data}
    )

    # driver.query('CREATE CONSTRAINT node_id IF NOT EXISTS FOR (n:Anything) REQUIRE n.ID IS UNIQUE')
    driver.query('CREATE CONSTRAINT node_id IF NOT EXISTS FOR (n:Anything) REQUIRE n.ID IS NODE KEY')
    driver.query('CREATE INDEX rel_id IF NOT EXISTS FOR ()-[r:Rel]->() ON (r.ID)')
    driver.query('CREATE LOOKUP INDEX rel_ty IF NOT EXISTS FOR ()-[r]->() ON type(r)')
    driver.query('CALL db.awaitIndexes()')

    for batch in chunks(nodes, batch_size):

        batch = [{'node_id': x} for x in batch]

        driver.query(
            """
            UNWIND $entities AS ent
            MERGE (n:Anything {ID: ent.node_id})
            """,
            parameters = {'entities': batch},
        )

    for batch in chunks(data, batch_size):

        driver.query(
            """
            UNWIND $entities AS ent
            MATCH (s:Anything {ID: ent.source_id})
            MATCH (t:Anything {ID: ent.target_id})
            MERGE (s)-[rel:Rel]->(t)
            set rel.ID = ent.ID
            """,
            parameters = {'entities': batch},
        )


def insert3(driver, edges = None, nodes = None, batch_size = 1.5e4, **kwargs):

    if not nodes or not edges:

        print('Generating random data')
        edges, nodes = random_rels(**kwargs)

    print('Grouping nodes and relations')
    nodes = by_label(nodes)
    edges = by_type(edges)

    print('Creating node indices')
    for label in nodes.keys():

        driver.query(
            f'CREATE INDEX node_id_{label.lower()} IF NOT EXISTS '
            f'FOR (n:{label}) ON (n.ID)'
        )

    print('Creating relation indices')
    for rtype in edges.keys():

        driver.query(
            f'CREATE INDEX rel_id_{rtype[0].lower()} IF NOT EXISTS '
            f'FOR ()-[r:{rtype[0]}]->() ON (r.ID)'
        )

    print('Deploying indices')
    driver.query('CALL db.awaitIndexes()')

    node_bar = tqdm.tqdm(
        desc = 'Inserting nodes',
        total = sum(map(len, nodes.values())),
    )

    for label, _nodes in nodes.items():

        _nodes = sorted(_nodes, key = lambda n: n['ID'])

        for batch in chunks(_nodes, batch_size, pbar = False):

            node_bar.set_description(f'Inserting {len(batch)} nodes')

            driver.query(
                'UNWIND $entities AS ent\n'
                f'MERGE (n:{label} '
                '{ID: ent.ID, '
                'prop0: ent.prop0, '
                'prop1: ent.prop1, '
                'prop2: ent.prop2})\n',
                parameters = {'entities': batch},
            )

            node_bar.update(n = len(batch))

    node_bar.close()

    edge_bar = tqdm.tqdm(
        desc = 'Inserting relations',
        total = sum(map(len, edges.values())),
    )

    for (rel_type, s_label, t_label), _edges in edges.items():

        _edges = sorted(_edges, key = lambda e: e['ID'])

        for batch in chunks(_edges, batch_size, pbar = False):

            edge_bar.set_description(f'Inserting {len(batch)} relations')

            driver.query(
                'UNWIND $entities AS ent\n'
                f'MATCH (s:{s_label} {{ID: ent.source_id}})\n'
                f'MATCH (t:{t_label} {{ID: ent.target_id}})\n'
                f'MERGE (s)-[rel:{rel_type}]->(t)\n'
                'SET\n'
                'rel.ID = ent.ID,\n'
                'rel.prop0 = ent.prop0,\n'
                'rel.prop1 = ent.prop1',
                parameters = {'entities': batch},
            )

            edge_bar.update(n = len(batch))

    edge_bar.close()


def main(driver_args = None, **kwargs):

    driver = neo4j_utils.Driver(**(driver_args or {}))
    print('Wiping database')
    driver.wipe_db()
    print('Dropping indices')
    driver.drop_indices_constraints()

    insert3(driver, **kwargs)
