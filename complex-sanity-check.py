import collections
import itertools
import sys

sys.path.append(".libs/")

import chronos

c = chronos.Client('127.0.0.1', 1992)
eva = c.create_event()
evb = c.create_event()
evc = c.create_event()
evd = c.create_event()
eve = c.create_event()
events = eva, evb, evc, evd, eve

rel = collections.defaultdict(lambda: collections.defaultdict(lambda: '?'))

def check_against_rel():
    rels_to_check = list(itertools.product(events, events))
    rels_stored = c.query_order(rels_to_check)
    for (x, y), (xp, yp, o) in zip(rels_to_check, rels_stored):
        assert x == xp
        assert y == yp
        assert rel[x][y] == o

check_against_rel()

assert c.assign_order([(eva, evb, '<')]) == [(eva, evb, '<')]
rel[eva][evb] = '<'
rel[evb][eva] = '>'
check_against_rel()

assert c.assign_order([(eva, evc, '<')]) == [(eva, evc, '<')]
rel[eva][evc] = '<'
rel[evc][eva] = '>'
check_against_rel()

assert c.assign_order([(eva, evd, '<')]) == [(eva, evd, '<')]
rel[eva][evd] = '<'
rel[evd][eva] = '>'
check_against_rel()

assert c.assign_order([(eva, eve, '<')]) == [(eva, eve, '<')]
rel[eva][eve] = '<'
rel[eve][eva] = '>'
check_against_rel()

assert c.assign_order([(evb, evc, '<')]) == [(evb, evc, '<')]
rel[evb][evc] = '<'
rel[evc][evb] = '>'
check_against_rel()

assert c.assign_order([(evb, evd, '<')]) == [(evb, evd, '<')]
rel[evb][evd] = '<'
rel[evd][evb] = '>'
check_against_rel()

assert c.assign_order([(evb, eve, '<')]) == [(evb, eve, '<')]
rel[evb][eve] = '<'
rel[eve][evb] = '>'
check_against_rel()

assert c.assign_order([(evc, evd, '<')]) == [(evc, evd, '<')]
rel[evc][evd] = '<'
rel[evd][evc] = '>'
check_against_rel()

assert c.assign_order([(evc, eve, '<')]) == [(evc, eve, '<')]
rel[evc][eve] = '<'
rel[eve][evc] = '>'
check_against_rel()

assert c.assign_order([(evd, eve, '<')]) == [(evd, eve, '<')]
rel[evd][eve] = '<'
rel[eve][evd] = '>'
check_against_rel()

c.release_references([eva])
for x in events:
    rel[eva][x] = 'X'
for x in events:
    rel[x][eva] = 'X'
check_against_rel()

c.release_references([evb])
for x in events:
    rel[evb][x] = 'X'
for x in events:
    rel[x][evb] = 'X'
check_against_rel()

c.release_references([evc])
for x in events:
    rel[evc][x] = 'X'
for x in events:
    rel[x][evc] = 'X'
check_against_rel()

c.release_references([evd])
for x in events:
    rel[evd][x] = 'X'
for x in events:
    rel[x][evd] = 'X'
check_against_rel()

c.release_references([eve])
for x in events:
    rel[eve][x] = 'X'
for x in events:
    rel[x][eve] = 'X'
check_against_rel()
