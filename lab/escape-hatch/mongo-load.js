// Lab DB-level load escape hatch (issue #87).
// Load into disposable Lab Mongo via compose exec mongosh — not a Lab Scenario.
// Seeds lab_escape_manual for Target-side DB inspect (mongosh). Product
// `migraloop target` covers Delivery-managed collections (e.g. lab_escape_customers
// after Oracle load + apply); use mongosh for raw Target-side loads like this.
db.lab_escape_manual.deleteMany({});
db.lab_escape_manual.insertMany([
  { _id: 1, note: "escape-hatch seed", source: "mongo-load.js" },
  { _id: 2, note: "inspect with mongosh against Lab Mongo URI", source: "mongo-load.js" },
]);
print("lab_escape_manual count=" + db.lab_escape_manual.countDocuments({}));
