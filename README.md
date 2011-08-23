Hoodie
======

This is an experimental fun project that implements nearest neighbour search in graph databases

The provided interfaces are graph database agnostic and currently the only provided implementation uses
tinkerpop's blueprint property graphs.

Hoodie stores an arbitrary set of records where each record is a multidimension value. The list of fields
of each record is described statically by creating a "Schema" using the chosen graph implementation's schema
factory.

Example.

val schema = schemaFactory.mkSchema(mkField[Int]("age"), mkField[Boolean]("gender"), ....)

The resulting schema instance provides insert, delete, and search calls for nearest neighbour search.


Approach of the blueprint implementation
----------------------------------------

Each value dimension is stored as a skip list in the graph database, i.e. skip list vertexes correspond to values
and link to predecessor, successor, upper level, and lower level vertexes as well as a double linked list
of record entry vertices that represent records that have that value as property in the skip list's dimension.
Such entry vertices have a successor pointer as well as a pointer to the actual record vertex they
represent. Records are stored as vertexes that link to exactly one entry vertex for each value dimension.

To say it the other way round: Records point to a single entry vertex for each value dimension. Entry vertexes
 point back to their record and are organized in a double linked list that dangles of a value vertex.
Value vertexes form a skip list together with other value records of their value dimension.

Records store a copy of all values they are associated with.

Finally, there is a global index that stores root pointers to all skip lists.


Nearest neighbour search in the blueprint implementation
--------------------------------------------------------

// These are old thoughts and do not match the current implementation

First, we define the notion of dimension neighbours. The dimension neighbours of a record are all records
sorted according to their distance to the source record in the fixed dimension only. This may be obtained
by using an iterator that starts at the value entry record of the source vertex and gradually moves
along the dimension's skip list in both directions.

To perform nearest neighbour search, we start at a source vertex.  Let the fringe iterators be the set of iterators
for the dimension neighbours of each of the source vertex' dimensions.

Now, repeat:

Expansion step: Get the next vertex from each fringe iterator.  Call the resulting set of vertexes the fringe.
Insert all fringe vertexes into a priority queue according to their distance to the source vertex.

Note: The priority queue may easily be bounded in size and just keep track of the top-k smallest records.

Extraction step:  Deliver all vertexes from the priority queue as a result that are stable.  A vertex is stable
if there is no iterator that could possibly produce a smaller vertex than the stable vertex in the future.

How to characterize stability?

Stability 1: A vertex is trivially stable if the minimum distance reached by all fringe iterators in their
dimension (!) is higher than the distance to the stable vertex.

Rationale: There is no way any vertex with lower distance will be found by further iteration.

Problem: Fails with dimensions with few values (i.e. boolean dimensions) as there will be very many
records with the same value in that dimension

Stability 2: A vertex is stable if for all dimensions, the distance reached by the corresponding
fringe iterator w.r.t this dimension only is higher than the distance of the stable vertex in that dimension and
the stable vertex is only preceded by other stable vertexes in the queue.

Rationale: Any vertex with a smaller value is found earlier.

Next, we try to generalize even further by considering the "distance socket" caused by iterator advancement.

Imagine stability 2 holds for all but a single dimension of a candidate vertex.











