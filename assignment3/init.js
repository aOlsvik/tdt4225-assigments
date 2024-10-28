db = db.getSiblingDB("db");  // Switch to the 'db' database

db.createUser({
    user: "user",
    pwd: "pswd",  
    roles: [{ role: "readWrite", db: "db" }]
});
