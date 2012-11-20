#!/usr/bin/env python
# -*- coding: utf-8 -*-
# filename: distribute_to_groups.py
#
#  2012, thatsokaybaby # public domain


import csv
import sqlite3

# TODO: Ausserhalb der Wünsche Kurse zuweisen.

# constant for maximal group size
MAX_SIZE = 20
# enable/disable printing of satisfied preference next to person's registration number
SHOW_PREF = False
# file for persons, file name
F_PERSON = 'person.csv'
# file for groups, file name
F_GROUP = 'group.csv'

# --- begin: SQL statement definitions---

SQL_INIT_DB = '''
-- table to hold the persons' data
CREATE TABLE person (
    name TEXT, 
    -- registration number, i.e. a student's registration number
    reg_num INTEGER,
    -- preferences for groups
    pref_1 INTEGER,
    pref_2 INTEGER,
    pref_3 INTEGER,
    CONSTRAINT person_const_1 PRIMARY KEY (reg_num)
);

-- table to hold the groups' data
CREATE TABLE grp (
    -- group is a SQL keyword therefore grp instead
    -- number to identify group
    num INTEGER,
    -- some name or description for the group
    name TEXT DEFAULT NULL,
    CONSTRAINT group_constr_1 PRIMARY KEY (num)
);

-- table to assign persons to groups (1:1)
CREATE TABLE person_in_grp (
    -- persons registration number, every person is in exactly one group if assigned
    p_reg_num INTEGER,
    -- group number
    g_num INTEGER,
    -- some comment for the assignment
    comment TEXT DEFAULT NULL,
    CONSTRAINT person_in_grp_constr_1 FOREIGN KEY (p_reg_num) REFERENCES person (reg_num) ON DELETE CASCADE,
    CONSTRAINT person_in_grp_constr_2 FOREIGN KEY (g_num) REFERENCES grp (num) ON DELETE CASCADE,
    CONSTRAINT person_in_grp_constr_3 UNIQUE (p_reg_num)
);

-- view for groups, their names, and number of members
CREATE VIEW grp_size AS
    SELECT
        g.num AS g_num,
        g.name AS g_name,
        count(g.num) AS members
    FROM person_in_grp AS pig
    JOIN grp AS g
        ON pig.g_num = g.num
    GROUP BY g.num, g.name
    ORDER BY count(g.num) DESC;

-- same as grp_sizes, but this time only overfull groups
CREATE VIEW grp_size_overfull AS
    SELECT
        gs.g_num,
        gs.g_name,
        gs.members
    FROM grp_size AS gs
    WHERE gs.members > {max_size:d}
    ORDER BY gs.members DESC;

-- same as grp_sizes, but this time only NOT full or overfull groups
CREATE VIEW grp_size_available AS
    SELECT
        gs.g_num,
        gs.g_name,
        gs.members
    FROM grp_size AS gs
    WHERE gs.members < {max_size:d}
    ORDER BY gs.members ASC;
'''

# SQL statement to get a count of preferences satisfied
SQL_SELECT_PREFS_SATISFIED = '''SELECT
    -- # of pref_1, pref_2, pref_3 satisfied
    count(nullif(p.pref_1 = pig.g_num, 0)),
    count(nullif(p.pref_2 = pig.g_num, 0)),
    count(nullif(p.pref_3 = pig.g_num, 0))
FROM person_in_grp AS pig
JOIN person AS p
    ON pig.p_reg_num=p.reg_num'''

# SQL statement to get members of a group
SQL_GRP_MEMBERS = '''SELECT
    p.reg_num,
    p.name,
    pig.comment
FROM person_in_grp AS pig
JOIN person AS p
    ON pig.p_reg_num = p.reg_num
WHERE pig.g_num = ?
ORDER BY p.reg_num ASC'''

# SQL statement for selecting the person in an overfull group that can be moved
# to a NOT full or overfull group while satisfying this person's pref_2 or
# pref_3, given by {0:s}.
# if used with cursor.fetchone() returns None if no more such person is
# available.
SQL_SELECT_PERSONS_1 = '''SELECT
    p.reg_num,
    p.{0:s}
FROM person_in_grp AS pig
JOIN person AS p
    ON pig.p_reg_num = p.reg_num
JOIN grp_size_available AS gsa
    ON p.{0:s} = gsa.g_num
JOIN grp_size_overfull AS gsof
    ON pig.g_num = gsof.g_num
ORDER BY gsof.members ASC LIMIT 1'''

# SQL statement for selection the person in an overfull group that can be moved
# to replace another person in another group that can be move into a NOT full
# or overfull group thus satisfying the person's (through {0:s}) and the other
# person's (through {1:s}) preferences.
# if used with cursor.fetchone() returns None if no more such person is
# available.
SQL_SELECT_PERSONS_2 = '''SELECT
    p_1.reg_num, -- person 1
    pig_2.g_num, -- group, person 1 can be re-assigned to
    p_2.reg_num, -- person 2
    p_2.{1:s} -- group, person 2 can be re-assigned to
FROM person_in_grp AS pig_1
JOIN person AS p_1
    ON pig_1.p_reg_num = p_1.reg_num
JOIN grp_size_overfull AS gsof_1
    ON pig_1.g_num = gsof_1.g_num
JOIN person_in_grp AS pig_2
    ON p_1.{0:s} = pig_2.g_num
JOIN person AS p_2
    ON pig_2.p_reg_num = p_2.reg_num
JOIN grp_size_available AS gsa
    ON p_2.{1:s} = gsa.g_num
LEFT OUTER JOIN grp_size_overfull AS gsof_2
    ON pig_2.g_num = gsof_2.g_num
WHERE gsof_2.g_num IS NULL
ORDER BY gsa.members ASC LIMIT 1'''

# SQL statement to insert a person into person table
SQL_INSERT_PERSON = 'INSERT INTO person (name, reg_num, pref_1, pref_2, pref_3) VALUES (?, ?, ?, ?, ?)'
# SQL statement to insert a group into grp table
SQL_INSERT_GRP = 'INSERT INTO grp (name, num) VALUES (?, ?)'
# SQL statement to assign every person its pref_1 group (initial setup), comment is set to "1"
SQL_INSERT_PERSON_IN_GRP = 'INSERT INTO person_in_grp (p_reg_num, g_num, comment) SELECT p.reg_num, p.pref_1, "1" FROM person AS p'
# SQL statement to assign a person a group (with a comment)
SQL_UPDATE_PERSON_IN_GRP_COMMENT = 'UPDATE person_in_grp SET g_num = ?, comment = ? WHERE p_reg_num = ?'
# SQL statement to select details on groups
SQL_SELECT_GRP = 'SELECT name, num FROM grp ORDER BY num ASC'
# SQL statement to select overfull groups
SQL_SELECT_GRP_SIZE_OVERFULL = 'SELECT g_num FROM grp_size_overfull'

# --- end: SQL statement definitions---

# db connection
#conn = sqlite3.connect('stud.sqlite')
conn = sqlite3.connect(':memory:')
c = conn.cursor()

# init db
c.executescript(SQL_INIT_DB.format(max_size=MAX_SIZE))

# load persons into db
with open(F_PERSON, 'r', encoding='utf-8') as f:
    for row in csv.reader(f, delimiter=';'):
        # [:5] if more than 5 fields are supplied, due to deprecated anmelde_r field
        c.execute(SQL_INSERT_PERSON, row[:5])
conn.commit()

# load groups into db
with open(F_GROUP, 'r', encoding='utf-8') as f:
    for row in csv.reader(f, delimiter=';'):
        c.execute(SQL_INSERT_GRP, row)
conn.commit()

# assign each person its initial group (which is pref_1)
c.execute(SQL_INSERT_PERSON_IN_GRP)
conn.commit()


# for convenience, meaningful constants
ACTION_MOVE = 1
ACTION_REPLACE = 2

# ACTION_MOVE = find a a person in an overfull group that can move to a not yet
# full group of his preference, if multiple exist, find the one that can move
# to the not yet full group with least members.
# ACTION_REPLACE = find a a person in an overfull group that can move to a
# group of his preference and replace another person which can then move to a
# not yet full group of his preference, if multiple exist, find the one that
# can replace the person to move to the not yet full group with least members.
# this could be done for n-th degree (but is not done in this script).
#
# order is determined by number and kind of pref-assignments created:
#
# pref_2, which is the best outcome
# pref_2 + pref_2,
# ref_3,
# pref_2 + pref_3,
# pref_3 + pref_2,
# and finally the worst, pref_3 + pref_3
for action, pref_a, pref_b in ((ACTION_MOVE, 'pref_2', None),
        (ACTION_REPLACE, 'pref_2', 'pref_2'), (ACTION_MOVE, 'pref_3', None),
        (ACTION_REPLACE, 'pref_2', 'pref_3'),
        (ACTION_REPLACE, 'pref_3', 'pref_2'),
        (ACTION_REPLACE, 'pref_3', 'pref_3')):
    if action == ACTION_MOVE:
        # find person that can move
        person = c.execute(SQL_SELECT_PERSONS_1.format(pref_a)).fetchone()
        # and do this as long as possible
        while person:
            # now re-assign the person
            c.execute(SQL_UPDATE_PERSON_IN_GRP_COMMENT, (person[1],
                    {'pref_2' : 2, 'pref_3' : 3}[pref_a], person[0]))
            conn.commit()
            # refresh
            person = c.execute(SQL_SELECT_PERSONS_1.format(pref_a)).fetchone()
    elif action == ACTION_REPLACE:
            # find a person that can replace another
            replace_persons = c.execute(SQL_SELECT_PERSONS_2.format(pref_a,
                    pref_b)).fetchone()
            # as long as there are persons to replace
            while replace_persons:
                # re-assign the persons
                c.execute(SQL_UPDATE_PERSON_IN_GRP_COMMENT, (replace_persons[1],
                        {'pref_2' : 2, 'pref_3' : 3}[pref_a],
                        replace_persons[0]))
                c.execute(SQL_UPDATE_PERSON_IN_GRP_COMMENT, (replace_persons[3],
                        {'pref_2' : 2, 'pref_3' : 3}[pref_b],
                        replace_persons[2]))
                conn.commit()
                # refresh
                replace_persons = c.execute(SQL_SELECT_PERSONS_2.format(pref_a,
                        pref_b)).fetchone()

# print groups, members
# iterate over groups
for g_name, g_num in c.execute(SQL_SELECT_GRP).fetchall():
    print('== ({0:d}) {1:s} =='.format(g_num, g_name.strip()))
    # iterate over members
    for p_reg_num, p_name, comment in c.execute(SQL_GRP_MEMBERS,
        (g_num,)).fetchall():
        if SHOW_PREF:
            print(p_reg_num, ' (', comment, ')', sep='')
        else:
            print(p_reg_num)
    print('\n', end='')

# this script does no distribution outside the preferences therefore overull
# groups may exist after distribution, re-assign manually by re-assigning
# persons to groups that are not full, beginning with persons assigned to their
# pref_3, then pref_2, finally pref_1, set SHOW_PREF = True to get information
# about satisfied preferences on each person
#
# warn about overfull groups only
overfull_groups = [(lambda x : x[0])(i) for i in c.execute(SQL_SELECT_GRP_SIZE_OVERFULL).fetchall()]
if overfull_groups:
    print('== Overfull groups ==')
    for g_num in overfull_groups:
        print(g_num)

print('group member maximum:', MAX_SIZE)
print('preferences sastisfied: {0:d}/{1:d}/{2:d}'.format(*c.execute(SQL_SELECT_PREFS_SATISFIED).fetchone()))

conn.close()