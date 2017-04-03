## Antidote SQL client

(For a restricted SQL-like interface on top of javascript).

Some explanations about the current architecture can be found in [architecture.md](./architecture.md).

### Limitations

- Only supports auto-increment keys
- Foreign keys can only reference primary keys
- Doesn't support nullable values
- `WHERE` predicates only support equality comparisons (see usage for details).
- Indices are not retroactive. They must be created before any inserts.
- No schema changes supported
- No DELETE support

### Usage

```js
const antidoteSQL = require('antidote_sql_client');

// Open a connection
const conn = antidoteSQL.connect(8087, '127.0.0.1');

// Create a new table
// NOTE: The first field will be chosen as the primary key
antidoteSQL.createTable(conn, "employee", [
    "empId",
    "name",
    "lastName",
    "username",
    "department"
])

antidoteSQL.createTable(conn, 'department', [
    'depId',
    'departmentName'
]);

// Create a foreign key
antidoteSQL.createFK(conn, "employee", {
    alias: "department",
    field_name: "depId",
    reference_table: "department"
})

// Create a new index
antidoteSQL.createIndex(conn, 'employee', {
    index_name: 'employee_name',
    field_names: 'name'
})

// Create a new unique index
antidoteSQL.createUniqueIndex(conn, 'employee', {
    index_name: 'employee_username',
    field_names: 'username'
})

// Inserts
antidoteSQL.insert(conn, 'department', {
    departmentName: 'sales'
})

antidoteSQL.insert(conn, 'employee', {
    username: "someUsername",
    name: "John",
    lastName: "Doe",
    department: 1
})

antidoteSQL.insert(conn, 'employee', {
    username: "anotherUsername",
    name: "Sally",
    lastName: "Mann",
    department: 1
})

// Selects

// select(conn, t, [f1, f2, ..., fn], predicate) will perform
// SELECT f1, f2, ..., fn FROM t where predicate = true
//
// The syntax for the predicate is
// { field_a: (value | values), field_b: (value | values), ...}
//
// This translates to
// SELECT [...]
//   FROM [...]
//  WHERE
//   field_a = value | field_a = value_1 OR value_2 OR ...
//   [AND field_b = value | field_b = value_1 OR value_2 OR ...]
//
// Currently we do not support OR between different fields (like A = B OR C = D).
//
// Supports for wildard select by calling `select(_, _, '*', _)`
// If no predicate is used, it will scan the whole table.

antidoteSQL.select(conn, 'employee', '*')
// Returns:
// [ { empId: 1,
//     name: "John",
//     lastName: "Doe", 
//     userName: "someUsername", 
//     department: 1 },
//   { empId: 2,
//     name: "Sally",
//     lastName: "Mann",
//     username: "anotherUsername",
//     department: 1 } ]

// Translates to
// SELECT name, lastName
//   FROM employee
//  WHERE name IN ("John", "Sally") AND department = 1
antidoteSQL.select(conn, 'employee', ['name', 'lastName'], {
    name: ["John", "Sally"],
    department: 1
})
// Returns:
// [ { name: "John", lastName: "Doe" }, { name: "Sally", lastName: "Mann" }]

// Joins
// Translates to
// SELECT *
//   FROM employee, department
//  WHERE employee.department = department.depId
antidoteSQL.join(conn, '*', ['employee', 'department'], ['department', 'depId']);
// Returns:
// [ { employee.empId: 1,
//     employee.name: "John",
//     employee.lastName: "Doe",
//     employee.userName: "someUsername",
//     employee.department: 1,
//     department.departmentName: "sales" },
//   { employee.empId: 2,
//     employee.name: "Sally",
//     employee.lastName: "Mann",
//     employee.username: "anotherUsername",
//     employee.department: 1,
//     department.departmentName: "sales" } ]

// Updates
// Translates to
// UPDATE employee
//    SET userName = aRealUserName
//  WHERE empId = 1
antidoteSQL.update(conn, 'employee', { userName: "aRealUserName" }, { empId: 1 });

// Close connection
antidoteSQL.close(conn)
```

### Transaction support

All operations described above are implicitly run in an Antidote
transaction. This means each of the above operations will return
an object `{ ct, result }` where `ct` is the commit time of the
transaction, and `result` contains the return value of the
transaction, if any. To chain together operations, one must
do so with promises. Like so:

```js
const conn = antidoteSQL.connect(8087, "127.0.0.1");

antidoteSQL.createTable(remote, "sampleTable", [
      "tableId",
      "tableFieldA",
]).then(_ => {
    return antidoteSQL.createIndex(remote, "sampleTable", {
        index_name: "someIndex",
        field_names: "tableFieldA"
    });
}).then(_ => {
    return antidoteSQL.insert(remote, "sampleTable", {
        tableFieldA: "someValue"
    });
})
```


### User-defined transactions

To run long-lived transactions, use the `runTransaction` method.
It will automatically start a new transaction for you, and commit
it as soon as the callback finishes. If any error is thrown inside
this function, the transaction will be aborted.

```js
const conn = antidoteSQL.connect(8087, '127.0.0.1');

antidoteSQL.runTransaction(conn, tx => {
    return antidoteSQL.createTable(tx, "tableFoo", [
            "fooId",
            "fooA",
            "fooB"
    ]).then(_ => {
        return antidoteSQL.createTable(tx, "tableBar", [
            "barId",
            "barA",
            "barB"
        ]);
    })
});
```
