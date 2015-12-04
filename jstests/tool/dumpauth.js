// dumpauth.js
// test mongodump with authentication

var m = MongoRunner.runMongod({auth: "", bind_ip: "127.0.0.1"});
var dbName = "admin"
var colName = "testcol"
db = m.getDB(dbName);

db.createUser({user:  "testuser" , pwd: "testuser", roles: jsTest.adminUserRoles});
assert( db.auth( "testuser" , "testuser" ) , "auth failed" );

t = db[colName];
t.drop();

db.setProfilingLevel(2);

for(var i = 0; i < 100; i++) {
  t.save({ "x": i });
}

db.createUser({user:  "backup" , pwd: "password", roles: ["backup"]});

x = runMongoProgram( "mongodump",
                     "--db", dbName,
                     "--authenticationDatabase=admin",
                     "-u", "backup",
                     "-p", "password",
                     "-h", "127.0.0.1:"+m.port);
assert.eq(x, 0, "mongodump should succeed with authentication");
